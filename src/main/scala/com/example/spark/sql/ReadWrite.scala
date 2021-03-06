package com.example.spark.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class ReadWrite {

  @Test
  def reader1(): Unit ={

    // 创建 sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 框架
    spark.read
  }

  @Test
  def reader2(): Unit ={

    // 创建 sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 第一种形式
    spark.read
      .format("csv")
      .option("header",value = true)
      .option("inferSchema",value = true)
      .load("src/main/resources/BeijingPM20100101_20151231.csv")
      .show(10)

    // 第二种形式
    spark.read
      .option("header",value = true)
      .option("inferSchema",value = true)
      .csv("src/main/resources/BeijingPM20100101_20151231.csv")
      .show(10)
  }

  @Test
  def writer1(): Unit ={

    // 创建 sparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 读取数据集
    val df = spark.read
      .option("header",value = true)
      .option("inferSchema",value = true)
      .csv("src/main/resources/BeijingPM20100101_20151231.csv")

    // 写入数据集
    df.write.json("src/main/resources/beijing_pm.json")

    df.write.format("json").save("src/main/resources/beijing_pm2.json")


  }

}
