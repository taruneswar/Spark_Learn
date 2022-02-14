package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CSV_Practise {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CSV_Practise").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
   Logger.getLogger("org").setLevel(Level.ERROR)
    import spark.implicits._
    import spark.sql
    //val chapterDataRDD = spark.read.option("header","false").csv("/home/taruneswar/Downloads/chapters-201108-004545.csv")
   val chapterDataRDD=sc.textFile("/home/taruneswar/Downloads/chapters-201108-004545.csv").map(x=>{
     (x.split(",")(0),x.split(",")(1))
   })
   val fil = chapterDataRDD.map(x=>(x._2,x._1)).zipWithIndex()
    val res=fil.filter(x=>(x._2!=0 && x._2!=1))
    val res1=res.map(x=>(x._1._1,x._1._2))
    res1.collect().foreach(println)
    spark.stop()
  }
}
