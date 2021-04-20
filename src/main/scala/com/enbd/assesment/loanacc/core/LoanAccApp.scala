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

object LoanAccApp {

  def main(args: Array[String]): Unit = {

    // Get spark Session
    val spark = getSparkSession

    // Get Spark Context
    val sc = spark.sparkContext

    // Get Streaming Context
    val ssc = new StreamingContext(sc, Seconds(60))

    // Set Checkpoint Dir
    ssc.checkpoint("""D:\Amruth\Softwares\spark\checkpoint""")

    // Set LogLevel to ERROR
    sc.setLogLevel("ERROR")

    //Define Kafka paramas
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "0001",
      "auto.offset.reset" -> args(0))

    //Initialize Topic names
    val loanTopic = Set("loan")
    val accountTopic = Set("account")

    //Initialize Loan DStream
    val loanDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      loanTopic)

    //Initialize Account DStream
    val accountDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      accountTopic)

    /* Update of the Existing State. The below definition gives the logic by which each attribute gets updated. */
    def updateState(key: Int, newValue: Option[(Int, Long, BigDecimal, Long)], state: State[(Int, Long, BigDecimal, Long)]): (Int, (Int, Long, BigDecimal, Long)) = {

      var currLoanCnt = Long.box(0)
      var currLoanAmt = BigDecimal(0)
      var currLastMinLoanCnt = BigDecimal(0)
      var updatedTotalCount = 0L
      var updatedTotalAmt = scala.math.BigDecimal(0)
      var lastMntCntCount = 0L

      //Check if state exists, if yes then fetch the state and initialize new values
      if (state.exists()) {
        val currentRow = state.get()
        currLoanCnt = currentRow._2
        currLoanAmt = currentRow._3
        currLastMinLoanCnt = currentRow._3

        updatedTotalCount = currLoanCnt + newValue.get._2
        updatedTotalAmt = currLoanAmt + newValue.get._3
        lastMntCntCount = newValue.get._4
      } else {
        updatedTotalCount = newValue.get._2
        updatedTotalAmt = newValue.get._3
        lastMntCntCount = newValue.get._4
      }
      // Generate the row
      val updatedRow = (key, updatedTotalCount, updatedTotalAmt, lastMntCntCount)

      // Update the state
      state.update(updatedRow)

      // Return the row
      (key, updatedRow)

    }

    // String Json value mapped to Account Schema
    val accountDS = accountDStream.map(kafkaPayload => (jsonToType[Account](kafkaPayload._2)))
    // String Json value mapped to Loan Schema
    val loanDS = loanDStream.map(kafkaPayload => (jsonToType[Loan](kafkaPayload._2)))

    // Create Key for joining
    val loanWithKey = loanDS.map(rec => (rec.AccountId, rec))
    val accountWithKey = accountDS.map(rec => (rec.AccountId, rec))

    //    val joinedStream = loanWithKey.window(Seconds(30)).join(accountWithKey.window(Seconds(30)))
    val joinedStream = loanWithKey.join(accountWithKey)

    // Group by account type and aggregate the values using function getAggregatedRow
    val groupedDStream = joinedStream.map(strm => (strm._2._2.AccountType, (strm._2._2.AccountType, strm._2._1.LoanId, strm._2._1.Amount))).groupByKey()
      .map(x => (x._1, getAggregatedRow(x._2)))

    // Current values are Mapped with the State values and updated accordingly.
    val output = groupedDStream.mapWithState(StateSpec.function(updateState _)).map(x => x._2)

    // Output print to console
    output.print()

    ssc.start()

    ssc.awaitTermination()
  }
}