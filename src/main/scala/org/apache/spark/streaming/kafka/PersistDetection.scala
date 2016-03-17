package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition

/**
 * @author ehl
 */
trait PersistDetection {
  /**
   * 获取offset 进行判断是否获取历史消息；并返回位置
   */
  def setOrUpdate(topics: Set[String],kafkaParams: Map[String, String]):Map[TopicAndPartition,Long]
}