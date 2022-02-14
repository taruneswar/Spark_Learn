package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object basic1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("basic1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val chapterDataRDD=sc.textFile("/home/taruneswar/Downloads/chapters-201108-004545.csv").map(x=>{
      (x.split(",")(0).toInt,x.split(",")(1).toInt)
    })
    val viewDataRDD=sc.textFile("/home/taruneswar/Downloads/views*.csv").map(x=>{
      (x.split(",")(0).toInt,x.split(",")(1).toInt)
    })
    val titlesDataRDD=sc.textFile("/home/taruneswar/Downloads/titles.csv").map(x=>{
      (x.split(",")(0).toInt,x.split(",")(1).toInt)
    })
    //ex-1
     val chapterCountRDD =chapterDataRDD.map(x=>(x._2,1)).reduceByKey((x,y) => x+y)

    //ex-2
    val viewDistinctRDD=viewDataRDD.distinct()
    val flipViewData=viewDistinctRDD.map(x=>(x._2,1))
    val joinedRDD=flipViewData.join(chapterDataRDD)
    val pairRDD=joinedRDD.map(x=>((x._2._1,x._2._2),1))
    val userPerCourse=pairRDD.reduceByKey(_+_)
    val courseViews=userPerCourse.map(x=>(x._1._2,x._2))
    val newJoinedRDD=courseViews.join(chapterCountRDD)
    val courseCompletion=newJoinedRDD.mapValues(x=>(x._1.toDouble/x._2))
    courseCompletion.collect().foreach(println)

    spark.stop()
  }
}
