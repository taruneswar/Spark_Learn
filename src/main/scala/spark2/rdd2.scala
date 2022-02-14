package spark2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
//import org.apache.spark.sql.types.StructType
case class person(name:String,age:Int,city:String)

object rdd2 extends App {
  val spark = SparkSession.builder.master("local[*]").appName("rdd2").getOrCreate()
  //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val sc = spark.sparkContext
  Logger.getLogger("org").setLevel(Level.ERROR)

  def ageCheck(age:Int):String ={
    if(age>18) "Yes" else "No"

  }

import spark.implicits._
  val e1=spark.read.csv("/home/taruneswar/test/P1.csv")
  val df=e1.toDF("name","age","city")
  //val ageFunc = udf(ageCheck(_:Int):String)
  //val df2=df.withColumn("Adult",ageFunc($"age"))

  //To Add this Data to SQL like and use the SQL Code in this by below code
  spark.udf.register("ageFunc",udf(ageCheck(_:Int):String))
  val df2=df.withColumn("adult",expr("ageFunc(age)"))
  df2.show()
  spark.catalog.listFunctions().filter(x=>x.name=="ageFunc").show()
  df2.createOrReplaceTempView("peoplestable")
  spark.sql("select name , age ,city,ageFunc(age) as adult from peoplestable").show()
  spark.stop()







}
