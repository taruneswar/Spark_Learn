package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Top_Movies extends App {
  val spark = SparkSession.builder.master("local[*]").appName("Top_Movies").getOrCreate()
  //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val sc = spark.sparkContext
  Logger.getLogger("org").setLevel(Level.ERROR)
  val ratingRdd = sc.textFile("/home/taruneswar/Downloads/ratings-201019-002101.dat")
  val movieRdd=sc.textFile("/home/taruneswar/Downloads/movies-201019-002101.dat")


  spark.stop()
}
