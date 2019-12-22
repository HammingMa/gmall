package com.mzh.bigdata.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.mzh.bigdata.gmall.bean.Option;
import com.mzh.bigdata.gmall.bean.OptionGroup;
import com.mzh.bigdata.gmall.bean.SaleDetailInfo;
import com.mzh.bigdata.gmall.service.PublisherService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.text.SimpleDateFormat;
import java.util.*;


@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("/realtime-total")
    public String getTotal(@RequestParam("date") String date){

        Integer total = publisherService.getTotal(date);

        List<Map> listMap = new ArrayList<Map>();

        Map<String,String> duaMap = new HashMap<String,String>();

        duaMap.put("id","dua");
        duaMap.put("name","新增日活");
        duaMap.put("value",total.toString());

        Map<String,String> midMap = new HashMap<String,String>();

        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value","20");

        Map<String,String> orderMap = new HashMap<String,String>();
        Integer order = publisherService.getOrder(date);
        orderMap.put("id","new_order");
        orderMap.put("name","新增交易额");
        orderMap.put("value",order.toString());

        listMap.add(duaMap);
        listMap.add(midMap);
        listMap.add(orderMap);


        return JSON.toJSONString(listMap);
    }

    @GetMapping("/realtime-hour")
    public String getHourTotal(@RequestParam("id") String id,@RequestParam("date") String date){

        Map map = new HashMap();
        if("dua".equals(id)){
            Map<String, Long> todayMap = publisherService.getHourTotal(date);
            String yesterday = getYesterday(date);
            Map<String, Long> yesterdayMap = publisherService.getHourTotal(yesterday);

            map.put("today",todayMap);
            map.put("yesterday",yesterdayMap);
        }else if("new_order".equals(id)){
            Map<String, Double> todayOrderMap = publisherService.getHourOrder(date);
            String yesterday = getYesterday(date);
            Map<String, Double> yesterdayOrderMap = publisherService.getHourOrder(yesterday);

            map.put("today",todayOrderMap);
            map.put("yesterday",yesterdayOrderMap);
        }

        return JSON.toJSONString(map);
    }

    public String getYesterday(String date){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        String yesterday = null;
        try {
            Date today = simpleDateFormat.parse(date);

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(today);

            calendar.add(Calendar.DATE,-1);

            yesterday = simpleDateFormat.format(calendar.getTime());


        }catch (Exception e){
            e.printStackTrace();
        }

        return yesterday;
    }

    @GetMapping("/sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String key,@RequestParam("startpage") int pageNo,@RequestParam("size") int pageSize){

        date = date.replaceAll("-","");

        Map user_gender = publisherService.getSaleDetail(date, key,pageNo , pageSize, "user_gender", 2);

        int total = (int)user_gender.get("total");

        Map<String,Long> genderMap = (Map<String,Long>)user_gender.get("groupMap");

        long maleCnt =genderMap.get("男");
        long femaleCnt=genderMap.get("女");

        double maleRadio = Math.round(maleCnt*1000D/total)/10D;
        double femaleRadio = Math.round(femaleCnt*1000D/total)/10D;

        List<Option> sexOptions = new ArrayList<Option>();

        sexOptions.add(new Option("男",maleRadio));
        sexOptions.add(new Option("女",femaleRadio));


        Map userAgeMap = publisherService.getSaleDetail(date, key, pageNo, pageSize, "user_age", 100);

        Map<String,Long> ageMap = (Map<String,Long>)userAgeMap.get("groupMap");

        Long age20cnt = 0L;
        Long age20_30cnt = 0L;
        Long age30cnt = 0L;

        for (Object o : ageMap.entrySet()) {
            Map.Entry<String,Long> entry = (Map.Entry<String,Long>) o;

            int age = Integer.parseInt(entry.getKey());
            Long cnt = entry.getValue();

            if(age<=20){
                age20cnt+=cnt;
            }else if(age<=30){
                age20_30cnt+=cnt;
            }else{
                age30cnt+=cnt;
            }
        }

        double age20Radio = Math.round(age20cnt*1000D/total)/10D;
        double age20_30Radio = Math.round(age20_30cnt*1000D/total)/10D;
        double age30Radio = Math.round(age30cnt*1000D/total)/10D;

        List<Option> ageOptions = new ArrayList<Option>();

        ageOptions.add(new Option("20以下",age20Radio));
        ageOptions.add(new Option("20-30",age20_30Radio));
        ageOptions.add(new Option("30以上",age30Radio));

        OptionGroup sexGroup = new OptionGroup("性别占比", sexOptions);
        OptionGroup ageGroup = new OptionGroup("年龄占比", ageOptions);

        List<OptionGroup> stat = new ArrayList<OptionGroup>();
        stat.add(sexGroup);
        stat.add(ageGroup);

        List<Map> detailList = (List<Map>) userAgeMap.get("detailList");
        SaleDetailInfo saleDetailInfo = new SaleDetailInfo(total, stat, detailList);


        return JSON.toJSONString(saleDetailInfo);
    }
}
