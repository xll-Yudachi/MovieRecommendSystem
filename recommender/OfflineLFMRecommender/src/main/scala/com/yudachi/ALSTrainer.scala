package com.yudachi

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * @Author Yudachi
 * @Description 模型评估和参数选取
 * @Date 2019/10/7 12:56
 * @Version 1.0
 **/
object ALSTrainer {

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )

        // 创建 SparkConf
        val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))

        //创建 SparkSession
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

        import sparkSession.implicits._

        //加载评分数据
        val ratingRDD: RDD[Rating] = sparkSession
            .read
            .option("uri", mongoConfig.uri)
            .option("collection", OfflineLFMRecommender.MONGODB_RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[MovieRating]
            .rdd
            .map(rating => Rating(rating.uid, rating.mid, rating.score))
            .cache()

        //将RDD分为两个RDD 一个是训练集一个是测试集
        val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8,0.2))

        //训练集
        val trainingRDD: RDD[Rating] = splits(0)
        //测试集
        val testingRDD: RDD[Rating] = splits(1)

        //输出最优参数
        adjustALSParams(trainingRDD, testingRDD)

        //关闭spark
        sparkSession.close()
    }

    //计算出最小均方根误差的参数
    def adjustALSParams(trainData:RDD[Rating], testData:RDD[Rating])={
        val result = for (rank <- Array(20,50,100,200); lambda <- Array(1, 0.1, 0.01, 0.001))
            yield {
                val model = ALS.train(trainData,rank,5,lambda)
                val rmse = getRMSE(model, testData)
                (rank, lambda, rmse)
            }
        // 按照rmse排序
        println(result.minBy(_._3))
    }

    //计算最小根误差
    def getRMSE(model : MatrixFactorizationModel, data : RDD[Rating])={
        //计算预测评分
        val userProducts = data.map(item => (item.user, item.product))
        val predictRating: RDD[Rating] = model.predict(userProducts)

        //以uid,mid作为外键，inner join实际观测值和预测值
        val observed: RDD[((Int, Int), Double)] = data.map(item => ((item.user, item.product), item.rating))
        val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))

        //内连接得到(uid,mid),(actual,predict)
        sqrt(
            observed.join(predict).map{
                case((uid, mid),(actual, pre)) => {
                    val err = actual - pre
                    err * err
                }
            }.mean()    //求平均值
        )
    }
}
