package com.jon.utils

import org.apache.spark.sql.SparkSession

object Utils {

    val Sql: SqlUtils = new SqlUtils

}

class SqlUtils {

    def ignoreCorruptFiles(spark: SparkSession): SqlUtils = {
        spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
        this
    }

    def ignoreMissingFiles(spark: SparkSession): SqlUtils = {
        spark.sql("set spark.sql.files.ignoreMissingFiles=true")
        this
    }
}

class OrcUtils extends SqlUtils {



}

class HiveUtils {



}
