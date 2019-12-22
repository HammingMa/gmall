package com.mzh.bigdata.gmall.common.util

import com.mzh.bigdata.gmall.constant.GmalConstant
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyElasticsearch {

  private var factory: JestClientFactory = null

  def getClient(): JestClient ={
    if(factory == null){
      bulder()
    }
    factory.getObject
  }

  def close(jestClient:JestClient): Unit ={
    if(jestClient!=null){
      try{
        jestClient.close()
      }catch {
        case e :Exception => e.printStackTrace()
      }

    }
  }

  def bulder(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://"+GmalConstant.ES_HOST + ":" + GmalConstant.ES_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000).build())
  }

  def main(args: Array[String]): Unit = {
    val client: JestClient = getClient()

    val source = "{\n  \"name\":\"lisi\",\n  \"age\":25,\n  \"amount\":34.5\n}"

    val index: Index = new Index.Builder(source)
      .index("gmall_test")
      .`type`("_doc")
      .build()

    client.execute(index)

    close(client)
  }


  def indexBulk(indexName:String,list:List[Any]): Unit ={
    val client: JestClient = getClient()

    val builder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

    for (source <- list) {
      val index: Index = new Index.Builder(source).build()
      builder.addAction(index)
    }

    val bulk: Bulk = builder.build()

    val result: BulkResult = client.execute(bulk)
    println(s"失败：${result.getFailedItems.size()}")

    println(s"提交：${result.getItems.size()}")

    close(client)

  }
}
