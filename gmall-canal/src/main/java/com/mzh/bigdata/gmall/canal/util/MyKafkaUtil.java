package com.mzh.bigdata.gmall.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaUtil {
    public static KafkaProducer<String,String> kafkaProducer = null;

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = new Properties();

        properties.put("bootstrap.servers","hdp1:9092,hdp2:9092,hdp3:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> kafkaProducer = null;

        try{
            kafkaProducer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }

        return  kafkaProducer;

    }

    public static void sender(String topic,String msg){
        if(kafkaProducer == null){
            kafkaProducer = createKafkaProducer();
        }

        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }
}
