package com.yudachi.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//redis操作返回的是java类，为了用map操作需要引入转换类
import scala.collection.JavaConversions._

object ConnHelper extends Serializable{
    lazy val jedis = new Jedis("hadoop2")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))

}

case class MongoConfig(uri:String,db:String)

// 标准 推荐
case class Recommendation(mid:Int, score:Double)

// 用户的推荐
case class UserRecs(uid:Int, recs:Seq[Recommendation])

// 电影的相似度
case class MovieRecs(mid:Int, recs:Seq[Recommendation])

object StreamingRecommender {
    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIM_MOVIES_NUM = 20
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //拿到streaming context
        val sc = sparkSession.sparkContext
        val streamingContext = new StreamingContext(sc,Seconds(2))

        implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        import sparkSession.implicits._



        //加载电影相似度矩阵数据，然后广播出去
        val simMovieMatrix: collection.Map[Int, Map[Int, Double]] = sparkSession.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRecs]
            .rdd
            .map {
                movieRecs => { //为了查询相似度方便 转换成map
                    (movieRecs.mid, movieRecs.recs.map(x => {
                        (x.mid, x.score)
                    }).toMap)
                }
            }
            .collectAsMap()

        //将电影相似度矩阵数据发布到每一个集群节点上
        val simMovieMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simMovieMatrix)

        //定义kafka连接参数
        val kafkaParams = Map(
            "bootstrap.servers" -> "hadoop2:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest"
        )

        //通过kafka创建一个DStream
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParams)
        )

        // 把原始数据UID|MID|SCORE|TIMESTAMP转换成评分流
        val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
            msg: ConsumerRecord[String, String] => {
                val attr = msg.value().split("\\|")
                (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
            }
        }

        // 继续做流式处理，核心实时算法部分
        ratingStream.foreachRDD{
            rdds => rdds.foreach{
                case (uid, mid, score, timestamp) => {
                    println("rating data coming! =====================")
                    //1. 从redis里获取当前用户最近的k次评分，保存成Array[(mid, score)]
                    val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

                    //2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表Array[mid]
                    val candidateMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

                    //3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐
                    val streamRecs: Array[(Int, Double)] = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)


                    //4. 把推荐数据保存到mongodb
                    saveDataToMongoDB(uid, streamRecs)
                }
            }
        }

        //开始接收和处理数据
        streamingContext.start()

        println(">>>>>>>>>>>>>> streaming started!")

        streamingContext.awaitTermination()

    }

    def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis)={


        // 从用户的队列中取出Num个评分
        jedis.lrange("uid:" + uid.toString, 0 ,num)
            .map{
                item =>{
                    val attr = item.split("\\:")
                    (attr(0).trim.toInt, attr(1).trim.toDouble)
                }
            }.toArray
    }

    def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
        //1. 从相似度矩阵中拿到所有相似的电影
        val allSimMovies: Array[(Int, Double)] = simMovies(mid).toArray

        //2. 从mongodb中查询用户已经看过的电影
        val ratingExist: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
            .find(MongoDBObject("uid" -> uid))
            .toArray
            .map {
                item => {
                    item.get("mid").toString.toInt
                }
            }

        // 3. 把看过的电影从相似度矩阵中过滤掉，得到输出列表
        allSimMovies
            .filter{
                movie => {
                    !ratingExist.contains(movie._1)
                }
            }
            //根据相似度矩阵的电影相似度排序
            .sortWith(_._2 > _._2)
            .take(num)
            .map{
                movie => {
                    movie._1
                }
            }
    }

    def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)], simMovies: collection.Map[Int, Map[Int, Double]]) = {
        // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
        val scores = collection.mutable.ArrayBuffer[(Int, Double)]()
        // 定义HashMap，保存每一个备选电影的增强减弱因子
        val increMap = collection.mutable.HashMap[Int, Int]()
        val decreMap = collection.mutable.HashMap[Int, Int]()

        for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
            // 拿到备选电影和最近评分电影的相似度
            val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1,simMovies)

            if (simScore > 0.7) {
                //计算备选电影的基础推荐得分
                scores += ((candidateMovie, simScore * userRecentlyRating._2))
                if (userRecentlyRating._2 > 3) {
                    increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
                } else {
                    decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
                }
            }
        }

        //根据备选电影的mid做groupby, 根据公式去求最后的推荐评分
        //groupby之后的数据格式为 Map[Int, ArrayBuffer[(Int, Double)]]
        scores
            .groupBy(_._1)
            .map{
                case (mid, scoreList) => {
                    (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid,1)) - log(decreMap.getOrDefault(mid,1)))
                }
            }
            .toArray
    }

    // 获取两个电影之间的相似度
    def getMoviesSimScore(mid1 : Int, mid2 : Int, simMovies: collection.Map[Int, Map[Int, Double]]) = {

        simMovies.get(mid1) match {
            case Some(sims) => sims.get(mid2) match {
                case Some(score) => score
                case None => 0.0
            }
            case None => 0.0
        }
    }

    //自定义Log可以指定底数 进行超参数的调优
    //底数默认为10
    def log(m: Double) = {
        val N = 10
        math.log(m) / math.log(N)
    }

    def saveDataToMongoDB(uid : Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) : Unit = {
        // 定义到StreamRecs表的连接
        val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

        // 如果表中已有uid对应的数据，则删除
        streamRecsCollection.findAndRemove(MongoDBObject("uid"->uid))

        // 将streamRecs数据存入表中
        streamRecsCollection.insert(
            MongoDBObject (
                "uid" -> uid,
                "recs" -> streamRecs.map {
                    movie => {
                        MongoDBObject("mid" -> movie._1, "score" -> movie._2)
                    }
                }
            )
        )

    }

}
