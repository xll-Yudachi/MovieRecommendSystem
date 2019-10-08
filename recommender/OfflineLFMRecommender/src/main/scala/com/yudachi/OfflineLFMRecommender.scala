package com.yudachi

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String,language: String, genres: String, actors: String, directors: String )

//基于评分数据的LFM 只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score:Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineLFMRecommender {

    // 定义常量
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    // 推荐表的名称
    val USER_RECS = "UserRecs"
    val MOVIE_RECS = "MovieRecs"
    val USER_MAX_RECOMMENDATION = 20


    def main(args: Array[String]): Unit = {
        // 定义配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //创建一个sparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineLFMRecommender")

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import sparkSession.implicits._

        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //加载数据
        val movieRatingRDD: RDD[(Int, Int, Double)] = sparkSession.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRating]
            .rdd
            .map(movieRating => (movieRating.uid, movieRating.mid, movieRating.score))
            .cache()

        //从movieRating数据中提取所有的uid和mid 去重
        val userRDD = movieRatingRDD.map(_._1).distinct()
        val movieRDD = movieRatingRDD.map(_._2).distinct()

        //训练隐语义模型
        val trainData = movieRatingRDD.map(x => Rating(x._1, x._2, x._3))

        //rank 隐特征维数 相当于k
        //iterations 定P求Q 定Q求P 的最小交替二乘法的迭代次数
        //lambda 正则化项系数
        val (rank, iterations, lambda) = (200, 5, 0.1)

        //训练模型
        val trainModel: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

        // 基于用户和电影的隐特征，计算预测评分，得到用户的推荐列表
        // 计算User和movie的笛卡尔积， 得到一个空评分矩阵
        val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

        //调用训练模型的predict方法 预测评分
        val preRatings: RDD[Rating] = trainModel.predict(userMovies)

        val userRecs: DataFrame = preRatings
            .filter(_.rating > 0) //过滤出评分大于0的项
            .map(rating => (rating.user, (rating.product, rating.rating)))
            .groupByKey()
            .map {
                case (uid, recs) => {
                    UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(data => {
                        Recommendation(data._1, data._2)
                    }))
                }
            }
            .toDF()

        userRecs.write
            .option("uri",mongoConfig.uri)
            .option("collection",USER_RECS)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()



        //计算电影相似度 为实时推荐做准备
        //获取电影的特征矩阵，数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
        val movieFeatures = trainModel.productFeatures.map {
            case (mid, features) => {
                (mid, new DoubleMatrix(features))
            }
        }

        //电影推荐列表
        //计算笛卡尔积 并过滤合并
        val movieRecs: DataFrame = movieFeatures.cartesian(movieFeatures)
            .filter {
                case (m1, m2) => {
                    m1._1 != m2._1
                }
            }
            .map {
                case (m1, m2) => {
                    val simScore = consinSim(m1._2, m2._2) //求两部电影的余弦相似度
                    (m1._1, (m2._1, simScore))
                }
            }
            .filter(_._2._2 > 0.6) //过滤出余弦相似度大于0.6的电影
            .groupByKey()
            .map {
                case (mid, movies) => {
                    MovieRecs(mid, movies.toList.sortWith(_._2 > _._2).map {
                        movie => {
                            Recommendation(movie._1, movie._2)
                        }
                    })
                }
            }
            .toDF()

        movieRecs.write
            .option("uri",mongoConfig.uri)
            .option("collection", MOVIE_RECS)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        sparkSession.close()
    }

    //计算两个电影之间的余弦相似度
    def consinSim(movie1 : DoubleMatrix, movie2 : DoubleMatrix) : Double = {
        movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
    }
}
