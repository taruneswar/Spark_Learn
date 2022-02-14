package spark2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType

object ex1 extends App {
  val spark = SparkSession.builder.master("local[*]").appName("ex1").getOrCreate()
  //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  val sc = spark.sparkContext
  Logger.getLogger("org").setLevel(Level.ERROR)

  val mylist=List(
    (1,"2013-07-25",11599,"CLOSED"),
    (2,"2013-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",12111,"COMPLETE"),
    (4,"2013-07-25",8827,"CLOSED")
  )

  import spark.implicits._
  val orderDF=spark.createDataFrame(mylist).toDF("orderid","orderdate","customerid","status")
  val newDF=orderDF.withColumn("orderdate",unix_timestamp(col("orderdate")
                   .cast(DateType)))
                   .withColumn("newid",monotonically_increasing_id())
    .dropDuplicates("orderdate","status")
    .drop("orderid")
    .sort("orderdate")
  newDF.show()

  spark.stop()

}
