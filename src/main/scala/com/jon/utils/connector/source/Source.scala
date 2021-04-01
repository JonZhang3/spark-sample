package com.jon.utils.connector.source

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.streaming.DataStreamReader

abstract class Source(private val reader: DataFrameReader,
                      private val sreader: DataStreamReader) {

    type R <: Source

    def load(): Unit

    def option(key: String, value: String): R = {
        if(reader != null) {
            reader.option(key, value)
        } else {
            sreader.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Long): R = {
        if(reader != null) {
            reader.option(key, value)
        } else {
            sreader.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Boolean): R = {
        if(reader != null) {
            reader.option(key, value)
        } else {
            sreader.option(key, value)
        }
        this[R]
    }

    def option(key: String, value: Double): R = {
        if(reader != null) {
            reader.option(key, value)
        } else {
            sreader.option(key, value)
        }
        this[R]
    }

}
