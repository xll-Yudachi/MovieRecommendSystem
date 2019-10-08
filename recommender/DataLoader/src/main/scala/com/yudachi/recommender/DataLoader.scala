package com.yudachi.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

// 定义样例类
//电影
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)
//评分
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
//标签
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
 * @Description MongoDB配置类
 * @Params  uri: uri链接
 * @Params  db: 数据库
 **/
case class MongoConfig(uri : String, db : String)

/**
  * @Description MongoDB配置类
  * @Params httpHosts: http主机列表 逗号分隔
  * @Params transportHosts: transportHosts主机列表
  * @Params  index: 需要操作的索引
  * @Params  clustername: 集群名称 默认elasticsearch 我配置的是escluster
  **/
case class ESConfig(httpHosts : String, transportHosts : String, index : String, clustername:String)

object DataLoader {

    //使用本地文件系统
    /*val MOVIE_DATA_PATH = " D:\\IDEAWorkSpace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = " D:\\IDEAWorkSpace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH = "D:\\IDEAWorkSpace\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"*/


    //使用hdfs文件系统
    val MOVIE_DATA_PATH = "hdfs://hadoop2:9000/MovieRecommendSystem/resources/movies.csv"
    val RATING_DATA_PATH = "hdfs://hadoop2:9000/MovieRecommendSystem/resources/ratings.csv"
    val TAG_DATA_PATH = "hdfs://hadoop2:9000/MovieRecommendSystem/resources/tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_MOVIE_INDEX = "Movie"

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender",
            "es.httpHosts" -> "hadoop2:9200",
            "es.transportHosts" -> "hadoop2:9300",
            "es.index" -> "recommender",
            "es.cluster.name" -> "escluster"
        )


        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import sparkSession.implicits._


        //加载数据
        val movieRDD: RDD[String] = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)

        val movieDF: DataFrame = movieRDD.map(
            item => {
                val attr = item.split("\\^")
                Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
                    attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
            }
        ).toDF()


        val ratingRDD: RDD[String] = sparkSession.sparkContext.textFile(RATING_DATA_PATH)

        val ratingDF: DataFrame = ratingRDD.map(
            item => {
                val attr = item.split(",")
                Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
            }
        ).toDF()

        val tagRDD: RDD[String] = sparkSession.sparkContext.textFile(TAG_DATA_PATH)

        val tagDF: DataFrame = tagRDD.map(
            item => {
                val attr = item.split(",")
                Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
            }
        ).toDF()


        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //将数据存储到MongoDB
        storeDataInMongoDB(movieDF,ratingDF,tagDF)

        //数据预处理

        /**
         *  得到电影类别表
         *  mid     tags
         *  电影ID   类别->以|分隔
         **/
        val newTag: DataFrame = tagDF.groupBy($"mid")
            .agg(concat_ws("|", collect_set($"tag")).as("tags"))
            .select("mid", "tags")

        //newTag和movie做join 数据合并 左外连接
        val movieWithTagsDF: DataFrame = movieDF.join(newTag,Seq("mid"),"left")


        implicit val esConfig = ESConfig(config("es.httpHosts"),config("es.transportHosts"),config("es.index"),config("es.cluster.name"))



        //保存数据到ES
        storeDataInES(movieWithTagsDF)

    }


    def storeDataInMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig) = {
        //新建一个mongodb连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //如果已经存在相应的数据库 先删除
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

        //将DF数据写入对应的mongodb表中
        movieDF.write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_MOVIE_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        ratingDF.write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        tagDF.write
            .option("uri",mongoConfig.uri)
            .option("collection",MONGODB_TAG_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        // 对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

        //关闭客户端
        mongoClient.close()
    }

    def storeDataInES(movieWithTagsDF : DataFrame)(implicit esConfig : ESConfig): Unit = {
        //新建一个配置
        val settings = Settings.builder().put("cluster.name",esConfig.clustername).build()

        //新建一个ES客户端
        val esClient = new PreBuiltTransportClient(settings)

        //将TransportHosts添加到esClient中
        val REGEX_HOST_PORT = "(.+):(\\d+)".r
        esConfig.transportHosts.split(",").foreach{
            case REGEX_HOST_PORT(host : String, port : String) => {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
            }
        }

        //需要清除ES中遗留的数据
        if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
            esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
        }

        //将数据写入到ES中
        movieWithTagsDF
            .write
            .option("es.nodes",esConfig.httpHosts)
            .option("es.http.timeout","100m")
            .option("es.mapping.id","mid")
            .mode("overwrite")
            .format("org.elasticsearch.spark.sql")
            .save(esConfig.index + "/" + ES_MOVIE_INDEX)
    }
}
