package cn.wchy.spark.day5

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  */
object StreamingWordCount {

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()
    //StreamingContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //累加的时候设置checkpoint
    ssc.checkpoint("/Users/wangchangye/WorkSpace/")
    //接收数据
    val ds = ssc.socketTextStream("10.69.42.22", 9999)
    //DStream是一个特殊的RDD
    //hello tom hello jerry
    /*val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    //打印结果
    result.print()
    ssc.start()
    ssc.awaitTermination()*/

    //累加

    val results = ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    results.print()
    ssc.start()
    ssc.awaitTermination()


  }

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }

}
