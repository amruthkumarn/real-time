package com.enbd.assesment.loanacc.transformers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Column
import com.enbd.assesment.loanacc.msg.Account
import org.apache.spark.sql.functions.from_json
import com.fasterxml.jackson.databind.ObjectMapper;


object processor {
  
  def getAggregatedRow(grp: Iterable[(Int, Long, BigDecimal)]): (Int, Long, BigDecimal, Long) = {
    val accType = grp.map(x => x._1).head
    val loanCnt = grp.map(x => x._2).size
//    val loanCnt1 = grp.map(x => x._2).foldLeft(Set(Long))((in, out) => in.+(out))
    //handle none case and duplicate cases
    val amt = grp.map(x => x._3).foldLeft(BigDecimal(0))(_ + _)
    (accType, loanCnt, amt, loanCnt)
  }
// 
  
}