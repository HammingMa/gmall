package com.mzh.bigdata.gmall.service;

import java.util.Map;

public interface PublisherService {

    public Integer getTotal(String date);

    public Map<String,Long> getHourTotal(String date);

    public Integer getOrder(String date);

    public Map<String,Double> getHourOrder(String date);

    public Map getSaleDetail(String date,String key,int pageNo,int pageSize,String aggsFieldName, int aggSize);
}
