package com.jon.utils.connector.sink

import org.apache.spark.sql.DataFrameWriter

class JdbcSink[T](val writer: DataFrameWriter[T])
    extends Sink[T](writer) {

    def url(url: String): JdbcSink[T] = {
        writer.option("url", url)
        this
    }

    def dbtable(table: String): JdbcSink[T] = {
        writer.option("dbtable", table)
        this
    }

    def batchsize(size: Long): JdbcSink[T] = {
        writer.option("batchsize", size)
        this
    }

    // NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE
    def isolationLevel(level: String): JdbcSink[T] = {
        writer.option("isolationLevel", level)
        this
    }

    def user(user: String): JdbcSink[T] = {
        writer.option("user", user)
        this
    }

    def password(password: String): JdbcSink[T] = {
        writer.option("password", password)
        this
    }

    override def save(): Unit = {
        writer.save()
    }
}
