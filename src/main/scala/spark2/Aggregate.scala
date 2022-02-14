package spark2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Aggregate extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder.master("local[*]").appName("Aggregate").getOrCreate()
  //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val sc = spark.sparkContext
  val df=spark.read.option("header",true).option("inferSchema",true).csv("/home/taruneswar/test/order_data-201025-223502.csv")
  df.select(
    count("*").as("total_rowcount"),sum("Quantity").as("TotalQuantity"),avg("UnitPrice")
      .as("AvgPrice"),countDistinct("InvoiceNo").as("CountDisctinct")
  ).show
  df.selectExpr(
    "count(*) as RowCount","sum(Quantity) as Total_quatity"
  ).show()
  df.createOrReplaceTempView("sales")
  spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()

  spark.stop()
}
