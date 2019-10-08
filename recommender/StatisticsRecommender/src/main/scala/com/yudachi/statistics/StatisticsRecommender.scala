package com.yudachi.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

//推荐基准类
case class Recommendation(mid:Int, score:Double)

case class GenresRecommendation(genres:String, recs:Seq[Recommendation])


object StatisticsRecommender {

    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    // 统计的表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )


        //创建SparkConf
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

        //创建一个SparkSession
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        //创建MongoConfig
        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        //支持SparkSQL的DF操作
        import sparkSession.implicits._

        //从Mongodb加载数据
        val ratingDF = sparkSession.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()

        val movieDF = sparkSession.read
            .option("uri", mongoConfig.uri)
            .option("collection", MONGODB_MOVIE_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Movie]
            .toDF()

        //创建名为ratings的临时表
        ratingDF.createOrReplaceTempView("ratings")

        //TODO: 不同的统计推荐结果
        //1. 历史热门统计，历史评分数据最多
        val rateMoreMoviesDF = sparkSession.sql("select mid, count(mid) as count from ratings group by mid")
        storeDFInMongoDB(rateMoreMoviesDF , RATE_MORE_MOVIES)

        //2. 近期热门统计，按照"yyyyMM"格式选取最近的评分数据，统计评分个数
        //创建一个日期格式化工具
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")

        //注册udf，把时间戳转换成年月格式
        sparkSession.udf.register("changeDate",(x : Int)=>{
            simpleDateFormat.format(new Date(x * 1000L)).toInt
        })

        //对原始数据做预处理，去掉uid
        val ratingOfYearMonth = sparkSession.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
        ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

        //从ratingOfMonth中查找电影在各月份的评分 mid, count, yearmonth
        val rateMoreRecentlyMoviesDF = sparkSession.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth,mid order by yearmonth desc, count desc")

        //存入Mongodb
        storeDFInMongoDB(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)


        //3. 优质电影统计，统计电影的平均评分
        val averageMoviesDF = sparkSession.sql("select mid, avg(score) as avg from ratings group by mid")
        storeDFInMongoDB(averageMoviesDF,AVERAGE_MOVIES)

        //4. 各类别电影Top统计
        //把平均分和电影表关联在一起 方便做top10统计
        val movieWithScore: DataFrame = movieDF.join(averageMoviesDF,"mid")

        //所有的电影类别
        val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery","Romance","Science","Tv","Thriller","War","Western")

        //将电影类别转换成RDD
        val genresRDD: RDD[String] = sparkSession.sparkContext.makeRDD(genres)

        //将电影类别和带有平均分的电影表做笛卡尔积
        //cartesian:  Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in `this` and b is in `other`.

        val genrenTopMovies: DataFrame = genresRDD.cartesian(movieWithScore.rdd)
            .filter {
                case (genre, movieWithScoreRow) => {
                    movieWithScoreRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
                }
            }
            .map {
                //case class Recommendation(mid:Int, score:Double)
                //目标格式=>(类型,(电影编号,平均分))
                case (genre, movieWithScoreRow) => {
                    (genre, (movieWithScoreRow.getAs[Int]("mid"), movieWithScoreRow.getAs[Double]("avg")))
                }
            }
            .groupByKey()
            .map {
                //数据源格式->(类型,RDD集合(电影编号,平均分))
                //目标格式 -> case class GenresRecommendation(genres:String, recs:Seq[Recommendation])
                case (genre, datas) => {
                    GenresRecommendation(genre, datas.toList.sortWith(_._2 > _._2).take(10).map(data => Recommendation(data._1, data._2)))
                }
            }
            .toDF()

        //保存到mongoDB中
        storeDFInMongoDB(genrenTopMovies,GENRES_TOP_MOVIES)

        sparkSession.stop()
    }

    def storeDFInMongoDB(df : DataFrame, collection_name : String)(implicit mongoConfig: MongoConfig)={
        df.write
            .option("uri",mongoConfig.uri)
            .option("collection",collection_name)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
    }
}
