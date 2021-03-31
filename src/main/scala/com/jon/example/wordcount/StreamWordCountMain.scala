package com.jon.example.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamWordCountMain {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[1]")
            .setAppName("StreamWordCount")
        val scc = new StreamingContext(conf, Seconds(2))
        scc.socketTextStream("localhost", 9000)
            .flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()
        scc.start()
        scc.awaitTermination()
    }

}
