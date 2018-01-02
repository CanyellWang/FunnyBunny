package cn.wchy.spark.day7

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wangchangye on 2017/4/27.
  */
object ScanPlugins {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(zkQuorum,group, topics, numThreads) = args
    val dateformat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("/Users/wangchangye/WorkSpace/log")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String]("metadata.broker.list" -> zkQuorum,
                                          "group.id" -> group,
                                          "auto.offset.reset" -> "smallest"
              )
    val dstream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams,topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2)
    val splitedlines = lines.map(_.split("\t"))
    val fileteredLines = splitedlines.filter(f => {

        val et = f(3)
        val item = f(8)
        et == "11" && item == "强效太阳水"
      }
    )
    fileteredLines.print()

    val window: DStream[(String, Iterable[Long])] = fileteredLines.map(
      f => (f(7), dateformat.parse(f(12)).getTime)

    ).groupByKeyAndWindow(Seconds(30))

    val filteredwindow: DStream[(String, Iterable[Long])] = window.filter(_._2.size >= 5)
    val avgValues: DStream[(String, Double)] = filteredwindow.mapValues(
      it => {
        val list = it.toList.sorted
        val size = list.size
        val first = list(0)
        val last = list(size - 1)
        val re: Double = last - first
        re / size
      }
    )
    //将平均吃药时间小于1000的user找出来
    val badUsers: DStream[(String, Double)] = avgValues.filter(_._2 < 1000)
    badUsers.foreachRDD(
      rdd =>{
        //存redis
        rdd.foreachPartition(
          it => {
            it.foreach({
              t =>{
                val ueser = t._1
                val avgtime = t._2
              }
            })
          }
        )
      }
    )


    ssc.start()
    ssc.awaitTermination()



  }//main

}//object
