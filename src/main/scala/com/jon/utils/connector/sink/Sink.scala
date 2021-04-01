package com.jon.utils.connector.sink

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.DataStreamWriter

abstract class Sink[T](private val writer: DataFrameWriter[T], private val swriter: DataStreamWriter[T]) {
    type R <: Sink[T]

    def save(): Unit

    def option(key: String, value: String): R = {
        if(writer != null) {
            writer.option(key, value)
        } else {
            swriter.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Long): R = {
        if(writer != null) {
            writer.option(key, value)
        } else {
            swriter.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Boolean): R = {
        if(writer != null) {
            writer.option(key, value)
        } else {
            swriter.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Double): R = {
        if(writer != null) {
            writer.option(key, value)
        } else {
            swriter.option(key, value)
        }
        this[R]
    }

}
