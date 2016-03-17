package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import kafka.message.MessageAndMetadata
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD

/**
 * @author ehl
 */
object EhlKafkaUtils {
  
  def apply(kafkaParams: Map[String, String]):EhlKafkaUtils={
      new EhlKafkaUtils(kafkaParams);
  }
  def apply(kafkaParams: Map[String, String],pType:String):EhlKafkaUtils={
    
    /**
      * 后期扩展
      */
     def getFacadePersist()={
       pType match {
         case "REDIS" =>{
           
         }
         
       }
     }
      new EhlKafkaUtils(kafkaParams,pType);
  }
  
    
}
/**
 *默认zk实现
 */
class EhlKafkaUtils(var kafkaParams: Map[String, String], pType:String="ZK") extends Serializable with FacadePersist{
     val logger = LoggerFactory.getLogger(getClass)
     val groupId=kafkaParams.getOrElse(EhlKafkaConstant.GROUP_ID, "spark-kafka-10000000"); 
     lazy val timeInterval=kafkaParams.getOrElse("spark.stream.kafka.time.interval", 6000L)//6000毫秒
     val kc = new KafkaCluster(kafkaParams)
     private var _lastUpdateMs=0L
     
     
    /**
     * spark 接受kafka 实时数据流,
     * 扩展功能：是否需要对spark消费kafka的数据进行记录
     * 默认不进行记录，与原生spark-kafka一致
     * 
     * @parameter isPersist :是否需要保存kafka消费记录
     */
    def createKafkaDirectStream[ K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag,VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics:Set[String]=Set.empty,
      isPersist:Boolean=false  
    ): InputDStream[(K, V)]={
        
          if(logger.isDebugEnabled()){
            logger.debug("create kafka direct straeam and used isPersist={}",isPersist)
          }
      
         isPersist match {
           case true=>{
           //处理kafka
           
             val offsets = setOrUpdate(topics, kafkaParams)
             val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
             KafkaUtils.createDirectStream(ssc, kafkaParams, offsets, messageHandler)
           }
           case false=>KafkaUtils.createDirectStream(ssc, kafkaParams, topics);
         }
    }
     /**
      * 获取offset 进行判断是否获取历史消息；并返回位置
      */
   override  def setOrUpdate(topics: Set[String],kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
         var map =Map[TopicAndPartition,Long]();
         topics.foreach { topic => {
        
              val partions=kc.getPartitions(Set(topic));
              
              if(partions.isLeft) throw new SparkException(s"get kafka partition failed:${partions.left.get}")
              val consumerOffsets=kc.getConsumerOffsets(groupId, partions.right.get);
              
              consumerOffsets match{
                    //初次获取
                    case Left(s)=>{
                      val reset = kafkaParams.getOrElse(EhlKafkaConstant.AUTO_OFFSET_RESET,EhlKafkaConstant.AUTO_OFFSET_RESET_LAGE_VALUE).toLowerCase()
                      var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
                      
                      logger.info("get kafka confumer offsets,resurn left: topic={},groupid={},and fetch kafka config:auto.offset.reset={}", topic,groupId,reset)
                      
                      if (reset == Some(EhlKafkaConstant.AUTO_OFFSET_RESET_SMALL_VALUE)) {
                        leaderOffsets = kc.getEarliestLeaderOffsets(partions.right.get).right.get
                      } else {
                        leaderOffsets = kc.getLatestLeaderOffsets(partions.right.get).right.get
                      }
                      val offsets = leaderOffsets.map {
                          case (tp, offset) => map+=(tp-> offset.offset);(tp, offset.offset)
                      }
                      kc.setConsumerOffsets(groupId, offsets)
                    }
                        
                    case Right(r)=>{
                      
                    /**
                     * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
                     * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
                     * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
                     * 这时把consumerOffsets更新为earliestLeaderOffsets
                     */
                    val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partions.right.get).right.get
                    r.foreach({ case(tp, n) =>
                        val earliestLeaderOffset =earliestLeaderOffsets(tp).offset
                        if (n < earliestLeaderOffset) {
                            logger.info("get kafka confumer offsets,resurn left: consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +" offsets已经过时，更新为" + earliestLeaderOffset)
                            map += (tp -> earliestLeaderOffset)
                            kc.setConsumerOffsets(groupId, Map(tp -> earliestLeaderOffset))
                         }else{
                            map+=(tp->n)
                         }
                     })
                    }
                  }
                } 
        }
        map
     }

   override def update[K :ClassTag, V :ClassTag](rdd: RDD[(K, V)]): Unit = {
        val current=System.nanoTime()
        if(current-_lastUpdateMs >= timeInterval.toString().toLong){
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offsets <- offsetsList) {
            val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
            val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
            if (o.isLeft) {
              println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
            }
        }
        _lastUpdateMs=current;
      }
  }
   
} 
