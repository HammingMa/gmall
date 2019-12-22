package com.mzh.bigdata.gmall.canal.handle;




import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;
import com.mzh.bigdata.gmall.canal.util.MyKafkaUtil;
import com.mzh.bigdata.gmall.constant.GmalConstant;


import java.util.List;

public class MyHandle {

    public static void handle(String tableName, CanalEntry.EventType type, List<CanalEntry.RowData> rowDatasList){
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(type)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : afterColumnsList) {

                    String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());

                    jsonObject.put(key,column.getValue());

                }

                System.out.println(jsonObject.toJSONString());

                MyKafkaUtil.sender(GmalConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());

            }
        }
    }
}
