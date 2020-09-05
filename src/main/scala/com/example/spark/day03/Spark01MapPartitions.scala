package com.example.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

object Spark01MapPartitions {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 以分区为单位，对RDD中的元素进行映射
    // 一般使用与批处理的操作，比如：将RDD中的元素插入到数据库中，需要数据库连接
    val newRdd = rdd.mapPartitions(datas => {
      datas.map(_ * 2)
    })

    newRdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
