package com.example.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 默认分区
 */
object Spark03PartitionDefault {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("Spark01_CreateRDD_mem").setMaster("local[*]")

    // 创建sparkcontext,该对象是提交spark App的入口
    val sc = new SparkContext(conf)

    // 通过集合创建RDD
    //val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 通过读取外部文件创建RDD
    val rdd = sc.textFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/input")
    // 查看分区效果
    //println(rdd.partitions.size)
    rdd.saveAsTextFile("/Users/yuwei1/Documents/scala/project/spark-studing/src/output")

    // 关闭连接
    sc.stop()
  }
}
