package com.mzh.bigdata.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mzh.bigdata.gmall.constant.GmalConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class LoggerController {

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;


    //自动装配
    @Autowired
    KafkaTemplate<String,String> kafka ;

    @PostMapping("/log")
    public String log(@RequestParam("log") String logString){

        JSONObject logJson = JSON.parseObject(logString);

        logJson.put("ts",System.currentTimeMillis());

        if("startup".equals(logJson.get("type"))){
            kafka.send(GmalConstant.KAFKA_TOPIC_STARTUP,logJson.toJSONString());
        }else {
            kafka.send(GmalConstant.KAFKA_TOPIC_EVENT,logJson.toJSONString());
        }


        logger.info(logJson.toJSONString());

        return "sucess";
    }
}
