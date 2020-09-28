package com.example.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
  @Test
  def dataset3(): Unit ={

    // 创建sparkSession
    val spark = new SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    val dataset = spark.createDataset(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    val executionRdd = dataset.queryExecution.toRdd

    val typeRdd = dataset.rdd

    println(executionRdd.toDebugString)

    println()
    println()

    println(typeRdd.toDebugString)
  }

  @Test
  def dataframe1(): Unit ={

    // 创建sparkSession
    val spark = SparkSession.builder().appName("dataframe1").master("local[6]").getOrCreate()

    // 创建dataframe
    import spark.implicits._
    val dataFrame: DataFrame = Seq(Person("zhangsan", 10), Person("lisi", 15)).toDF()

    // dataframe操作
    dataFrame.where('age >10)
      .select('name)
      .show()
  }

  @Test
  def dataframe2(): Unit ={

    // 创建sparkSession
    val spark = SparkSession.builder().appName("dataframe1").master("local[6]").getOrCreate()

    // 创建dataframe
    import spark.implicits._
    val personList = Seq(Person("zhangsan", 10), Person("lisi", 15))

    // to df
    val df1 = personList.toDF()

    val df2 = spark.sparkContext.parallelize(personList).toDF()

    val df3 = spark.createDataFrame(personList)


    val df4 = spark.read.csv("src/main/resources/BeijingPM20100101_20151231.csv")
    df4.show()
  }

  @Test
  def dataframe3(): Unit ={

    // 创建sparkSession
    val spark = SparkSession.builder().appName("pm analysis").master("local[6]").getOrCreate()

    import spark.implicits._
    // 读取数据集
    val sourceDF = spark.read.option("header",value = true).csv("src/main/resources/BeijingPM20100101_20151231.csv")

    // 查看dataframe的schema信息
    sourceDF.printSchema()

    // 处理
    //sourceDF.select("year","month","PM_Dongsi").
    //  where('PM_Dongsi =!= "NA").
    //  groupBy('year,'month).
    //  count()
    //  .show()

    // 将表注册为临表
    sourceDF.createOrReplaceTempView("pm")

    // 执行查询
    val resultDF = spark.sql("select year,month, count(PM_Dongsi) from pm where PM_Dongsi !='NA' group by  year,month")

    resultDF.show()

    // 得出结论
    spark.stop()
  }

  @Test
  def dataframe4(): Unit ={
    // 创建sparkSession
    val spark = SparkSession.builder().appName("dataframe1").master("local[6]").getOrCreate()

    // 创建dataframe
    import spark.implicits._
    val personList = Seq(Person("zhangsan", 10), Person("lisi", 15))

    val df = personList.toDF()

    df.map((row:Row) => Row(row.get(0),row.getAs[Int](1)*2))(RowEncoder.apply(df.schema)).show()

    val ds = personList.toDS()
    ds.map((person:Person) => Person(person.name,person.age * 2)).show()
  }
}
