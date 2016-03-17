package org.kafka.spark.plugin

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.StreamingContext

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
     val conf= ConfigFactory.load("test.conf")
     println(conf.getString("a"))
//     StreamingContext.getOrCreate(checkpointPath, creatingFunc, hadoopConf, createOnError)
  }

}
