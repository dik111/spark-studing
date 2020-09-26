package com.example.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.junit
import org.junit.Test

case class Person(name:String,age:Int)

class Intro {



  @Test
  def dsIntro(): Unit ={


    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    import spark.implicits._
    val personDs = sourceRDD.toDS()

    val resultDs = personDs.where('age > 10)
      .where('age < 20)
      .select('name)
      .as[String]

    resultDs.show()
  }

  @Test
  def dfIntro(): Unit ={
    val spark = new SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    import spark.implicits._
    val df = sourceRDD.toDF()

    df.createOrReplaceTempView("person")

    val resultDf = spark.sql("select name from person where age >10 and age <20")

    resultDf.show()
  }

  @Test
  def dataset1(): Unit ={

    // 创建sparkSession
    val spark = new SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    // 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

    // dataset 支持强类型的API
    dataset.filter(item => item.age>10).show()

    // dataset可以直接编写SQL表达式
    dataset.filter("age >10").show()
  }
}