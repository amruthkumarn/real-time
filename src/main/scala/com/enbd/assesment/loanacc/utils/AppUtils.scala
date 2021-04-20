package com.enbd.assesment.loanacc.utils

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ State, StateSpec, Time }


object AppUtils {

  def jsonToType[T](json: String)(implicit m: Manifest[T]): T = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue[T](json)
  }
  
  
  def getSparkSession : SparkSession = {
    val spark = SparkSession
    .builder()
    .appName("Spark_Streaming_LoanAccountApp")
    .getOrCreate();
    
    spark
  }
  
  
}