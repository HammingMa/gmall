package com.mzh.bigdata.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.mzh.bigdata.gmall.common.util.MyElasticsearch
import com.mzh.bigdata.gmall.constant.GmalConstant
import com.mzh.bigdata.gmall.realtime.bean.Order
import com.mzh.bigdata.gmall.realtime.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderAPP {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("order app").setMaster("local[1]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val recordDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafakStream(GmalConstant.KAFKA_TOPIC_ORDER,ssc)


    val orderDStream: DStream[Order] = recordDStream.map {
      case record => {
        val order: Order = JSON.parseObject(record.value(), classOf[Order])

        //电话号码脱敏

        val head: String = order.consigneeTel.substring(0,3)
        val tail: String = order.consigneeTel.substring(6, 10)

        order.consigneeTel = head + "****" + tail

        //转化时间
        val dateArr: Array[String] = order.createTime.split(" ")
        order.createDate = dateArr(0)
        val timeArr: Array[String] = dateArr(1).split(":")
        order.createHour = timeArr(0)
        order.createHourMinute = timeArr(0) + timeArr(1)

        println(order.consigneeTel)

        order
      }
    }


    orderDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(orders =>{
        val orderList: List[Order] = orders.toList
        //插入到es
        MyElasticsearch.indexBulk(GmalConstant.ES_INDEX_ORDER,orderList)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
