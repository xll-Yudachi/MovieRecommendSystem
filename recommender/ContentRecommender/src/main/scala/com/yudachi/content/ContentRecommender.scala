package com.yudachi.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String,language: String, genres: String, actors: String, directors: String )

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score:Double)


// 定义基于电影内容信息提取出的电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {

    //定义表名和常量
    val MONGODB_MOVIE_COLLECTION = "Movie"

    val CONTENT_MOVIE_RECS = "ContentMovieRecs"

    val USER_MAX_RECOMMENDATION = 20

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import sparkSession.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


        //加载数据 并做预处理
        val movieTagsDF = sparkSession
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .map {
                movie => {
                    //提取电影编号、电影名称、电影类别作为原始内容特征 分词器默认根据空格分词
                    (movie.mid, movie.name, movie.genres.map {
                        character => if (character == '|') ' ' else character
                    })
                }
            }
            .toDF("mid", "name", "genres")
            .cache()

        //TODO： 用TF-IDF从内容信息中提取电影特征向量
        //创建一个分词器 默认按空格分词
        val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

        // 用分词器对原始数据做转换，生成新的一列words
        val wordsData: DataFrame = tokenizer.transform(movieTagsDF)

        //引入HashingTF工具，可以把一个词语序列转化成对应的词频
        val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)


        //[adventure, animation, children, comedy, fantasy] (50,[11,13,19,43,49],[1.0,1.0,1.0,1.0,1.0])
        //输出的是 稀疏向量的数据结构
        //(50,[11,13,19,43,49],[1.0,1.0,1.0,1.0,1.0])
        //第一个数据是稀疏向量的总维度 第二个数据是每个分类对应的稀疏向量的位置 第三个数据是对应位置的值为1 其他位置的值为0
        val featurizeData: DataFrame = hashingTF.transform(wordsData)

        //引入IDF工具，可以得到idf模型
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        //训练idf模型，得到每个词的逆文档评率
        val iDFModel = idf.fit(featurizeData)
        //用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
        val rescaleData: DataFrame = iDFModel.transform(featurizeData)

        //rescaleData.show(truncate = false)

        val movieFeatures: RDD[(Int, DoubleMatrix)] = rescaleData
            .map {
                row => {
                    (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
                }
            }
            .rdd
            .map {
                //转换成(mid,特征向量组)的数据格式
                x => (x._1, new DoubleMatrix(x._2))
            }

        // 对所有的电影计算相似度
        val movieRecs: DataFrame = movieFeatures.cartesian(movieFeatures)
            .filter {
                case (a, b) => {
                    a._1 != b._1
                }
            }
            .map {
                case (a, b) => {
                    val simScore = consinSim(a._2, b._2)
                    (a._1, (b._1, simScore))
                }
            }
            .filter(_._2._2 > 0.6) //过滤出相似度大于0.6的
            .groupByKey()
            .map {
                case (mid, movies) => {
                    MovieRecs(mid, movies.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map {
                        x => Recommendation(x._1, x._2)
                    })
                }
            }
            .toDF()

        movieRecs
            .write
            .option("uri",mongoConfig.uri)
            .option("collection", CONTENT_MOVIE_RECS)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        sparkSession.stop()
    }

    // 求向量余弦相似度
    def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix)={
        movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
    }


}
