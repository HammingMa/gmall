package com.mzh.bigdata.gmall.realtime.util

import java.{io, lang, util}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}



object KafkaUtil {
  private val properties: util.Properties = PropertiesUtil.load("config.properties")

  private val brokerList: String = properties.getProperty("kafka.broker.list")

  private val kafkaParms = Map(
    ("bootstrap.servers", brokerList), //用于初始化链接到集群的地址
    ("key.deserializer", classOf[StringDeserializer]),
    ("value.deserializer", classOf[StringDeserializer]),
    ("group.id", "gmall_consumer_group"), //用于标识这个消费者属于哪个消费团体
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    ("auto.offset.reset", "latest"),
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    ("enable.auto.commit", (true: lang.Boolean))
  )


  def getKafakStream(topic:String,ssc:StreamingContext): InputDStream[ConsumerRecord[String, String]] ={
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic),
        kafkaParms)
    )
    dStream
}


}
