package com.jon.example.topn

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TopNMain {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[1]")
            .setAppName("topn-rdd")
        val ctx = new SparkContext(conf)
        val data = Seq(
            ("A", "Tom", 78),
            ("B", "James", 47),
            ("A", "Jim", 43),
            ("C", "James", 89),
            ("A", "Lee", 93),
            ("C", "Jim", 65),
            ("A", "James", 10),
            ("C", "Lee", 39),
            ("B", "Tom", 99),
            ("C", "Tom", 53),
            ("B", "Lee", 100),
            ("B", "Jim", 100)
        )
        val result = ctx.parallelize(data)
            .groupBy(_._1)
            .map(data => {
                val values = data._2
                val tuples = values.toList.sortWith((a, b) => a._3.compareTo(b._3) > 0).take(3)
                tuples
            }).flatMap(_.toBuffer).collect()
        println(result.mkString("\n"))
    }

}
