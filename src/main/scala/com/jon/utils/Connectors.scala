package com.jon.utils

import com.jon.utils.connector.sink.{JdbcSink, TextSink}
import com.jon.utils.connector.source.TextSource
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

object Connectors {

    private val FORMATER_CSV = "csv"
    private val FORMATER_JDBC = "jdbc"
    private val FORMATER_TEXT = "text"

    def text(reader: DataFrameReader): TextSource = {
        reader.format(FORMATER_TEXT)
        new TextSource(reader)
    }

    def text[T](writer: DataFrameWriter[T]): TextSink[T] = {
        writer.format(FORMATER_TEXT)
        new TextSink[T](writer)
    }

    def parquet(reader: DataFrameReader): Unit = {

    }

    def parquet[T](writer: DataFrameWriter[T]): Unit = {

    }

    def csv(reader: DataFrameReader): Unit = {
        reader.format(FORMATER_CSV)
    }

    def csv[T](writer: DataFrameWriter[T]): Unit = {
        writer.format(FORMATER_CSV)
    }

    def jdbc(reader: DataFrameReader): Unit = {
        reader.format(FORMATER_JDBC)
    }

    def jdbc[T](writer: DataFrameWriter[T]): JdbcSink[T] = {
        writer.format(FORMATER_JDBC)
        new JdbcSink[T](writer)
    }

}
