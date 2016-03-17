package org.apache.spark.streaming.kafka

/**
 * @author ehl
 */
object EhlKafkaConstant {
  val GROUP_ID="group.id"
  //"auto.offset.reset","largest"
  
  //name
  val AUTO_OFFSET_RESET="auto.offset.reset"
  //value
  val AUTO_OFFSET_RESET_LAGE_VALUE="largest"
  val AUTO_OFFSET_RESET_SMALL_VALUE ="smallest"
}