package com.jon.utils

import com.jon.utils.connector.sink.{JdbcSink, KafkaSink, TextSink}
import com.jon.utils.connector.source.{KafkaSource, TextSource}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

object Connectors {

    val FORMATER_CSV = "csv"
    val FORMATER_JDBC = "jdbc"
    val FORMATER_TEXT = "text"
    val FORMATER_KAFKA = "kafka"

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

    def kafka(reader: DataFrameReader): KafkaSource = {
        new KafkaSource(reader = reader)
    }

    def kafkaStream(reader: DataStreamReader): KafkaSource = {
        new KafkaSource(sreader = reader)
    }

    def kafka[T](writer: DataFrameWriter[T]): KafkaSink[T] = {
        new KafkaSink[T](writer)
    }

    def kafka[T](writer: DataStreamWriter[T]): KafkaSink[T] = {
        new KafkaSink[T](swriter = writer)
    }

}
