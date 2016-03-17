package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**
 * @author ehl
 * 自己实现
 */
trait PersistUpdate {
  def update[K:ClassTag, V:ClassTag](rdd: RDD[(K, V)])
}