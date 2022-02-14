package spark2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import spark2.rdd2.spark

object basic_DF extends App {
  val spark = SparkSession.builder.master("local[*]").appName("basic_DF").getOrCreate()
  //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val sc = spark.sparkContext
  Logger.getLogger("org").setLevel(Level.ERROR)

  val e1=spark.read.option("header",true).csv("/home/taruneswar/test/orders-201019-002101.csv")
  e1.createOrReplaceTempView("orders")
  val res = spark.sql("select order_customer_id,count(*) from orders where order_status = 'CLOSED' group by order_customer_id order by order_customer_id")
  res.show
  spark.stop()
}
