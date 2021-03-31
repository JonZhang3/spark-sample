package com.jon.example.wordcount

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("KafkaStreamWordCount")
        val ssc = new StreamingContext(conf, Seconds(2))
        ssc.checkpoint("file:///Users/jon/Documents/code/spark-sample/src/main/resources/test")
        val kafkaConfg = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = Array("test_spark")
        KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaConfg))
            .map(_.value())
            .flatMap(_.split(" "))
            .map((_, 1))
            .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
                val currentValue = values.sum
                val oldValue = state.getOrElse(0)
                Some(currentValue + oldValue)
            })
            .saveAsTextFiles("file:///Users/jon/Documents/code/spark-sample/src/main/resources/streamwordcount")
        ssc.start()
        ssc.awaitTermination()
    }

}
