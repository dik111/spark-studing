package com.example.spark.day01

import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建sparkConf配置文件
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    // 创建sparkContext对象
    val sc = new SparkContext(conf)
//
//    val textRDD = sc.textFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/input")
//
//    val flatMapRDD = textRDD.flatMap(_.split(" "))
//
//    val mapRDD = flatMapRDD.map((_, 1))
//
//    val reduceRDD = mapRDD.reduceByKey(_ + _)

    // 一行代码搞定
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args(1))

//    val result = reduceRDD.collect()
//
//    result.foreach(println)

    sc.stop()
  }
}
