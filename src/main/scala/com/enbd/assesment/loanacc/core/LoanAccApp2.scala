package com.enbd.assesment.loanacc.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{ State, StateSpec, Time }
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.ConstantInputDStream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.StringDecoder

import com.enbd.assesment.loanacc.transformers.Processor._
import com.enbd.assesment.loanacc.msg._
import com.enbd.assesment.loanacc.utils.AppUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object LoanAccApp2 {

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    val loanDStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "loan")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor")
      .option("auto.offset.reset", "largest")
      .load()
      .withColumn("loanProcessingTime", current_timestamp)

    val accountDStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "account")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor")
      .option("auto.offset.reset", "largest")
      .load()
      .withColumn("accProcessingTime", current_timestamp)

    val AccountSchema = StructType(Array(StructField("AccountId", LongType, true), StructField("AccountType", IntegerType, true)))

    val LoanSchema = StructType(Array(StructField("LoanId", LongType, true), StructField("AccountId", LongType, true), StructField("Amount", DecimalType(20, 5), true)))

    val accountDS = accountDStream.alias("a").selectExpr("CAST(value AS STRING)", "accProcessingTime").select(from_json(col("value"), AccountSchema) as "accountData", col("accProcessingTime")).withWatermark("accProcessingTime", "30 seconds")

    val loanDS = loanDStream.selectExpr("CAST(value AS STRING)", "loanProcessingTIme").select(from_json(col("value"), LoanSchema) as "loanData", col("loanProcessingTIme")).withWatermark("loanProcessingTIme", "30 seconds")

    // Join based on the processing time with Interval
    val loanAccDF = loanDS.join(accountDS, expr("""loanData.AccountId = accountData.AccountId AND loanProcessingTIme >= accProcessingTime AND   loanProcessingTIme <= accProcessingTime + interval 30 seconds"""), "leftouter").selectExpr("loanData.AccountId as AccountId", "accountData.AccountType", "loanData.Amount", "loanData.LoanId", "loanProcessingTIme")

    val loanDF = loanAccDF.selectExpr("AccountType", "LoanId", "loanProcessingTIme")

    // GroupBy Accounttype to get running aggregated values
    val loanAccAgg1 = loanAccDF.groupBy(col("AccountType")).agg(count(col("LoanId")).as("TotalCount"), sum(col("Amount")).as("TotalAmount"))

    // GroupBy with window to get number of Loans in Last minute
    val loanAccAgg2 = loanAccDF.groupBy(col("AccountType"), window(col("loanProcessingTIme"), "1 minute")).agg(count(col("LoanId")).as("LastMinuteCount"))

    // Attempt was made to check if the joining of streams with differnt groupBy (One - AccountType unbounded output and another - window)
    val finalAggregatedData = loanAccAgg1.alias("a").join(loanAccAgg2.alias("b"), loanAccAgg1("AccountType") <=> loanAccAgg2("AccountType"), "inner").selectExpr("a.AccountType", "a.TotalCount", "a.TotalAmount", "b.LastMinuteCount")
    /* This was unsuccessfull to join got error -------->  "Multiple streaming aggregations are not supported with streaming DataFrames/Datasets"
    -- The above code will help us join based on the Watermark and maintain running total (which is most desired.
  */

    finalAggregatedData.alias("a").select(to_json(struct("a.AccountType", "a.TotalCount", "a.TotalAmount")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "output")
      .option("checkpointLocation", """D:\Amruth\Softwares\spark\checkpoint""")
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}