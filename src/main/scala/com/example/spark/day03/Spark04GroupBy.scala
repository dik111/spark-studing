package com.example.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 对集合中的元素进行分组处理
 */
object Spark04GroupBy {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8),numSlices = 3)

    val newRdd = rdd.groupBy(_ % 2)

    rdd.mapPartitionsWithIndex((index,datas) =>{
      println(index + "---->"+datas.mkString(","))
      datas
    })


    newRdd.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
