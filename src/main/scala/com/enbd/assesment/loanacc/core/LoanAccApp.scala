package com.enbd.assesment.loanacc.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.{ State, StateSpec, Time }
import org.apache.spark.sql.types._
//import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ConstantInputDStream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka._

import com.enbd.assesment.loanacc.transformers.processor._
import com.enbd.assesment.loanacc.msg._
import com.enbd.assesment.loanacc.utils.AppUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf


object LoanAccApp {

  
 def main(args: Array[String]): Unit = {
   

  val spark = getSparkSession
 
//  val conf = new SparkConf().setAppName("asfd").setMaster("local[2]")

/*   val sparkConf = new SparkConf.setAppName("LoanAccApp")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
*/  
  val sc = spark.sparkContext
//  import spark.implicits._

  val ssc = new StreamingContext(sc, Seconds(30))
ssc.checkpoint("""D:\Amruth\Softwares\spark\checkpoint""")
sc.setLogLevel("ERROR")
  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "0001",
    "auto.offset.reset" -> args(0))
 
 
  val loanTopic = Set("loan")
  val accountTopic = Set("account")

  val loanDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    kafkaParams,
    loanTopic)

  val accountDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    kafkaParams,
    accountTopic)
    
    
 /* Update of the Existing State. The below definition gives the logic by which each attribute gets updated. */
val updateState = (key: Int, newValue: Option[(Int, Long, BigDecimal, Long)], state: State[(Int, Long, BigDecimal, Long)]) => {

     var currLoanCnt = Long.box(0)
    var currLoanAmt = BigDecimal(0)
    var currLastMinLoanCnt = BigDecimal(0)
    
    val currentRow = Some(state.get())
     currLoanCnt = currentRow.get._2
     currLoanAmt = currentRow.get._3
     currLastMinLoanCnt = currentRow.get._3

    val updatedTotalCount = currLoanCnt + newValue.get._2
    val updatedTotalAmt = currLoanAmt + newValue.get._3
    val lastMntCntCount = newValue.get._4

    val updatedRow = (key, updatedTotalCount, updatedTotalAmt, lastMntCntCount)
    state.update(updatedRow)
    Some(key, newValue, updatedRow)
  }

  /*
   * val accountDS = accountDStream.map(kafkaPayload => (jsonToType[Account](kafkaPayload._2), current_timestamp().as("accProcessingTime")))

  val loanDS = loanDStream.map(kafkaPayload => (jsonToType[Loan](kafkaPayload._2), current_timestamp().as("loanProcessingTime")))
*/

    val accountDS = accountDStream.map(kafkaPayload => (jsonToType[Account](kafkaPayload._2)))

  val loanDS = loanDStream.map(kafkaPayload => (jsonToType[Loan](kafkaPayload._2)))
  
 /* val loanWithKey = loanDS.map(rec => (rec._1.AccountId, rec))

  val accountWithKey = accountDS.map(rec => (rec._1.AccountId, rec))
*/
  
    val loanWithKey = loanDS.map(rec => (rec.AccountId, rec))

  val accountWithKey = accountDS.map(rec => (rec.AccountId, rec))

  val joinedStream = loanWithKey.window(Seconds(30)).join(accountWithKey.window(Seconds(30)))
// parked with some doubt 
  
  val groupedDStream = joinedStream.map(strm => (strm._2._2.AccountType, (strm._2._2.AccountType, strm._2._1.LoanId, strm._2._1.Amount))).groupByKey()
    .map(x => (x._1, getAggregatedRow(x._2)))
  
//  val groupedDStream = joinedStream.map(strm => (strm._2._2._1.AccountType, (strm._2._2._1.AccountType, strm._2._1._1.LoanId, strm._2._1._1.Amount))).groupByKey()
//    .map(x => (x._1, getAggregatedRow(x._2)))
    //namng of amoount can be changed
    
    
   val stateSpec = StateSpec.function(updateState)
   

  val output = groupedDStream.mapWithState(stateSpec)

  output.print()  
  
  ssc.start()

  ssc.awaitTermination()
}
}