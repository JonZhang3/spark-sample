package com.jon.utils.connector.source

import org.apache.spark.sql.DataFrameReader

abstract class Source(private val reader: DataFrameReader) {
    def load(): Unit
}
