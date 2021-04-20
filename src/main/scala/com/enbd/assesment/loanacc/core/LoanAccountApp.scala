/*package com.enbd.assesment.loanacc.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ State, StateSpec, Time }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.kafka.common.serialization.StringDeserializer
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._

object LoanAccountApp  {

  val spark = SparkSession
    .builder()
    .appName("Spark_Streaming_LoanAccountApp")
     .enableHiveSupport()
    .getOrCreate();

  val updateState = (key: Int, newValue: Option[Row], state: State[Row]) => {
    val currentRow = state.get()
    val updatedTotalCount = currentRow.getAs[Int]("TotalCount") + newValue.get(1).asInstanceOf[Int]
    val updatedTotalAmount = currentRow.getAs[Decimal]("TotalAmount") + newValue.get(2).asInstanceOf[Decimal]
    val updatedRow = Row(key, updatedTotalCount, updatedTotalAmount, currentRow.getAs[Int]("LastMinuteCount"))
    state.update(updatedRow)
    Some(key, newValue, updatedRow)
  }

  val sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(30))


  val spec = StateSpec.function(updateState)
  //val mappedStatefulStream = sessions.mapWithState(spec)

  val stateSpec = StateSpec.function(updateState)

  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> "node6:9092",
    "group.id" -> "0001",
    "auto.offset.reset" -> "latest")

  
     val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node6:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0001",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

  val loanTopic = Set("loan")
  val accountTopic = Set("account")

  val loanDStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    kafkaParams,
    loanTopic)

  val accountDStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc,
    kafkaParams,
    accountTopic)

  import spark.implicits._

  val AccountSchema = StructType(Array(StructField("AccountId", LongType, true), StructField("AccountType", IntegerType, true)))
  val LoanSchema = StructType(Array(StructField("LoanId", LongType, true), StructField("AccountId", LongType, true), StructField("Amount", DecimalType(20, 5), true)))


  val accountDS = accountDStreams.map(kafkaPayload => (kafkaPayload._2, current_timestamp().as("accProcessingTime")))
  accountDS.foreachRDD(rd => {
  }
  )
  
  
//  val accountProcessed = 
  
  val accountDStream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

  val accountDS1 = accountDStream.alias("a").selectExpr("CAST(value AS STRING)", "accProcessingTime").select(from_json($"value", AccountSchema) as "accountData", $"accProcessingTime").withWatermark("accProcessingTime", "30 seconds")

  // accountDS.writeStream    .format("console")    .outputMode("update")    .start()    .awaitTermination()
  val loanDStream = spark.readStream.format("socket").option("host", "localhost").option("port", 9998).load().withColumn("loanProcessingTIme", current_timestamp)

  val loanDS = loanDStream.selectExpr("CAST(value AS STRING)", "loanProcessingTIme").select(from_json($"value", LoanSchema) as "loanData", $"loanProcessingTIme").withWatermark("loanProcessingTIme", "30 seconds")

  //val loanAccDF = loanDS.join(accountDS, loanDS("loanData.AccountId")<=> accountDS("accountData.AccountId"), "leftouter")    .selectExpr("loanData.AccountId as AccountId", "accountData.AccountType", "loanData.Amount")

  val loanAccDF = loanDS.join(accountDS, expr("""loanData.AccountId = accountData.AccountId AND loanProcessingTIme >= accProcessingTime AND   loanProcessingTIme <= accProcessingTime + interval 30 seconds"""), "leftouter").selectExpr("loanData.AccountId as AccountId", "accountData.AccountType", "loanData.Amount")

  val loanAccAgg = loanAccDF.groupBy($"AccountType").agg(count($"AccountId").as("TotalCount"), sum($"Amount").as("TotalAmount")).select('AccountType, 'TotalCount, 'TotalAmount, 'TotalCount.as("LastMinuteCount"))

  val x = loanAccAgg.map(row => (row.getAs[Int]("AccountType"), row))

}*/