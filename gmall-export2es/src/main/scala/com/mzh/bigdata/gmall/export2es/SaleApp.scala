package com.mzh.bigdata.gmall.export2es

import com.mzh.bigdata.gmall.common.util.MyElasticsearch
import com.mzh.bigdata.gmall.constant.GmalConstant
import com.mzh.bigdata.gmall.export2es.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object SaleApp {

  def main(args: Array[String]): Unit = {



    val sparkConf: SparkConf = new SparkConf().setAppName("saleapp").setMaster("local[1]")

    val ss: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()



    var dt : String = "";

    if(args.length>0){
      dt = args(0)
    }else{
      dt="20191221"
    }

    val df: DataFrame = ss.sql("" +
      "select\n  user_id,\n  sku_id,\n  if(user_id =='0001',user_gender,'女') as user_gender,\n  if(user_id =='0001',18,if(user_id=='0002',28,40)) user_age,\n  user_level,\n  cast(100.6 as double) as sku_price,\n  sku_name,\n  sku_tm_id,\n  sku_category3_id,\n  sku_category2_id,\n  sku_category1_id,\n  sku_category3_name,\n  sku_category2_name,\n  sku_category1_name,\n  spu_id,\n  sku_num,\n  cast(order_count as bigint) order_count,\n  cast(order_amount as double) order_amount,\n  dt\nfrom\n  gmall_dws.sale_detail_daycount" +
      " where dt = '" + dt + "'")

    import ss.implicits._
    val saleRDD: RDD[SaleDetailDaycount] = df.as[SaleDetailDaycount].rdd

    saleRDD.foreachPartition(sales =>{
      val list: ListBuffer[SaleDetailDaycount] = new ListBuffer[SaleDetailDaycount]()

      for (sale <- sales) {
        list+=sale

        if(list.size==100){
          MyElasticsearch.indexBulk(GmalConstant.ES_INDEX_SALE,list.toList)
          list.clear()
        }
      }

      if(list.size >0){
        MyElasticsearch.indexBulk(GmalConstant.ES_INDEX_SALE,list.toList)
      }

    })

  }

}
