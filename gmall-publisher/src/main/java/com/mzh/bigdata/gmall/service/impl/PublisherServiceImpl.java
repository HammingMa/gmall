package com.mzh.bigdata.gmall.service.impl;

import com.mzh.bigdata.gmall.common.util.MyElasticsearch;
import com.mzh.bigdata.gmall.constant.GmalConstant;
import com.mzh.bigdata.gmall.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    JestClient jest;

    private void setJestServer() {
        HashSet<String> set = new HashSet<String>();
        set.add("http://hdp2:9200");
        jest.setServers(set);
    }


    @Override
    public Integer getTotal(String date) {
        setJestServer();

        String query = "";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));

        searchSourceBuilder.query(boolQueryBuilder);


        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmalConstant.ES_INDEX_STARTUP).addType("_doc").build();


        Integer total = 0;
        try {

            SearchResult result = jest.execute(search);
            System.out.println(result.getJsonString());
            System.out.println(result.getErrorMessage());
            total = result.getTotal().intValue();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return total;
    }


    @Override
    public Map<String, Long> getHourTotal(String date) {

        setJestServer();

        String groupbyName = "groupby_logHour";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));

        searchSourceBuilder.query(boolQueryBuilder);

        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms(groupbyName).field("logHour").size(24);

        searchSourceBuilder.aggregation(aggBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmalConstant.ES_INDEX_STARTUP).addType("_doc").build();


        Map<String, Long> map = new HashMap<String, Long>();
        try {

            SearchResult result = jest.execute(search);
            System.out.println(result.getJsonString());
            System.out.println(result.getErrorMessage());

            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation(groupbyName).getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                map.put(bucket.getKeyAsString(), bucket.getCount());
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    @Override
    public Integer getOrder(String date) {
        setJestServer();


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));

        searchSourceBuilder.query(boolQueryBuilder);


        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmalConstant.ES_INDEX_ORDER).addType("_doc").build();


        Integer total = 0;
        try {

            SearchResult result = jest.execute(search);
            System.out.println(result.getJsonString());
            System.out.println(result.getErrorMessage());
            total = result.getTotal().intValue();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return total;
    }

    @Override
    public Map<String, Double> getHourOrder(String date) {

        setJestServer();


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("createDate", date));

        searchSourceBuilder.query(boolQueryBuilder);


        String groupbyKey="groupby_createHour";
        String sumKey="sum_totalAmount";

        TermsAggregationBuilder createHour = AggregationBuilders.terms(groupbyKey).field("createHour");

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum(sumKey).field("totalAmount");

        createHour.subAggregation(sumAggregationBuilder);

        searchSourceBuilder.aggregation(createHour);


        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmalConstant.ES_INDEX_ORDER).addType("_doc").build();




        Map<String, Double> map = new HashMap<String, Double>();

        try {

            SearchResult result = jest.execute(search);
            System.out.println(result.getJsonString());
            System.out.println(result.getErrorMessage());


            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation(groupbyKey).getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKeyAsString();
                Double sum = bucket.getSumAggregation(sumKey).getSum();

                map.put(key,sum);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    @Override
    public Map getSaleDetail(String date, String key, int pageNo, int pageSize, String aggsFieldName , int aggSize) {
        setJestServer();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));

        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",key).operator(Operator.AND));


        String groupbyKey="groupby_"+aggsFieldName;
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms(groupbyKey).field(aggsFieldName).size(aggSize);

        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.aggregation(aggregationBuilder);

        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);


        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmalConstant.ES_INDEX_SALE).addType("_doc").build();

        int total = 0;

        List<Map> detailList = new ArrayList<>();

        Map<String,Long> groupMap = new HashMap<>();

        try{
            SearchResult result = jest.execute(search);

            total = result.getTotal().intValue();

            List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);

            System.out.println(hits.size());

            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }

            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation(groupbyKey).getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                groupMap.put(bucket.getKeyAsString(),bucket.getCount());
            }


        }catch (Exception e){
            e.printStackTrace();
        }

        Map map = new HashMap();

        map.put("total",total);

        map.put("detailList",detailList);
        map.put("groupMap",groupMap);

        return map;
    }
}
