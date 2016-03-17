package org.apache.spark.streaming.kafka

/**
 * @author ehl
 * 
 * 暴漏接口，
 * 自己检测zk位置
 * 并记录zk位置
 */
trait FacadePersist extends PersistDetection with PersistUpdate