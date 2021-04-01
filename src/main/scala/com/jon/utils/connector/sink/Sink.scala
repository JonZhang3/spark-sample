package com.jon.utils.connector.sink

import org.apache.spark.sql.DataFrameWriter

abstract class Sink[T](private val writer: DataFrameWriter[T]) {

    def save(): Unit

}
