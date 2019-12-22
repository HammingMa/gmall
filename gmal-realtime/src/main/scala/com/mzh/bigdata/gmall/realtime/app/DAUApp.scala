package com.mzh.bigdata.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mzh.bigdata.gmall.common.util.MyElasticsearch
import com.mzh.bigdata.gmall.constant.GmalConstant
import com.mzh.bigdata.gmall.realtime.bean.StartUpLog
import com.mzh.bigdata.gmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DAUApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DAUApp")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafakStream(GmalConstant.KAFKA_TOPIC_STARTUP,ssc)

    val startUpLogDStream: DStream[StartUpLog] = recordDstream.map(record => {
      val startUpLog: StartUpLog = JSON.parseObject[StartUpLog](record.value(), classOf[StartUpLog])

      val date: Date = new Date(startUpLog.ts)
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:MM").format(date)

      val dateArray: Array[String] = dateStr.split(" ")
      val timeArray: Array[String] = dateArray(1).split(":")

      startUpLog.logDate = dateArray(0)
      startUpLog.logHour = timeArray(0)
      startUpLog.logHourMinute = dateArray(1)

      startUpLog
    })





    //批次之间的去重
    val distinctDstream: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient
      val key: String = "DAU:" + curdate
      val duaSet: util.Set[String] = jedis.smembers(key)
      val scDua: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(duaSet)

      println(rdd.count())
      val filerRDD: RDD[StartUpLog] = rdd.filter(starUpLog => {
        !scDua.value.contains(starUpLog.mid)
      })

      println(filerRDD.count())
      filerRDD
    })


    //批次内部去重
    val groupByDstream: DStream[(String, Iterable[StartUpLog])] = distinctDstream.map(startUpLog => {
      (startUpLog.mid, startUpLog)
    }).groupByKey()

    val flatMapDStream: DStream[StartUpLog] = groupByDstream.flatMap {
      case (mid, array) => {
        array.take(1)
      }
    }


    flatMapDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(startUpLogs =>{
        val jedis: Jedis = RedisUtil.getJedisClient

        val list: List[StartUpLog] = startUpLogs.toList

        MyElasticsearch.indexBulk(GmalConstant.ES_INDEX_STARTUP,list)

        for (startUpLog <- list) {
          val key: String = "DAU:"+startUpLog.logDate
          jedis.sadd(key,startUpLog.mid)
        }

        jedis.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
