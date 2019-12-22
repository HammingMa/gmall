package com.mzh.bigdata.gmall.canal;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mzh.bigdata.gmall.canal.handle.MyHandle;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hdp2", 11111), "example", "", "");

        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();

            if(size==0){
                System.out.println("休息一下");
                try {
                    Thread.sleep(5000);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(CanalEntry.EntryType.TRANSACTIONBEGIN.equals(entry.getEntryType()) || CanalEntry.EntryType.TRANSACTIONEND.equals(entry.getEntryType())){
                        continue;
                    }

                    ByteString storeValue = entry.getStoreValue();

                    try {
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        String tableName = entry.getHeader().getTableName();

                        MyHandle.handle(tableName,rowChange.getEventType(),rowDatasList);

                    }catch (InvalidProtocolBufferException e){
                        e.printStackTrace();
                    }

                }
            }

        }
    }
}
