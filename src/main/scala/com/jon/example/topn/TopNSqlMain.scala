package com.jon.example.topn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.StructType

object TopNSqlMain {

    def main(args: Array[String]): Unit = {
        val session = SparkSession.builder().master("local[1]")
            .config("", value = true)
            .appName("topn-sql")
            .getOrCreate()
        import session.implicits._
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
        val df = session.sparkContext.parallelize(data).toDF("category", "name", "score")
        df.createOrReplaceTempView("person")
        val spec = Window.partitionBy($"category").orderBy($"score".desc)
        df.withColumn("topn", row_number().over(spec))
            .select($"category", $"name", $"score")
            .where(col("topn") < 4)
            .show()
//        session.sql("select category, name,score, rank from (SELECT category,name,score, row_number() over(partition by category sort by score desc) rank FROM person) t where t.rank < 4")
//            .show()
    }

}
