package com.jon.example.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountMain {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
          .setMaster("local[1]")
          .setAppName("WordCount")
        val context = new SparkContext(conf)
        context.setLogLevel("error")
        val tuples = context.textFile("hdfs://192.168.33.16:9000/word.txt")
          .flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey((a, b) => a + b)
          .collect()
        println(tuples.mkString("Array(", ", ", ")"))
    }

}
