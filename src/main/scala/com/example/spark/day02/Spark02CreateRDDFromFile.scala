package com.example.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过读取外部文件来创建RDD
 */
object Spark02CreateRDDFromFile {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("Spark01_CreateRDD_mem").setMaster("local[2]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    // 从本地文件中读取数据，创建RDD
    //val rdd = sc.textFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/input/1.txt")
    //rdd.collect().foreach(println)

    // 从hdfs中读取
    val rdd = sc.textFile("hdfs://master.sfygroup.com:8020/user/suofy/input")
    rdd.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
