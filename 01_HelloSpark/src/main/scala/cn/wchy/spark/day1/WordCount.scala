package cn.wchy.spark.day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object WordCount {
  def main(args: Array[String]) {
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
      .setJars(Array("C:\\HelloSpark\\target\\hello-spark-1.0.jar"))
      .setMaster("spark://node-1.itcast.cn:7077")
    val sc = new SparkContext(conf)

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitionsRDD
    val key: RDD[(String, Int)] = sc.textFile(args(0)).cache()
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_ + _)

      //产生一个RDD: mapPartitions
      //.saveAsTextFile(args(1))
    sc.stop()

    /*//非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))
    sc.stop()*/
  }
}
