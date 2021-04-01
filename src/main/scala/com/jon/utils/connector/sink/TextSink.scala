package com.jon.utils.connector.sink

import org.apache.spark.sql.DataFrameWriter

class TextSink[T](val writer: DataFrameWriter[T])
    extends Sink[T](writer) {

    private var path: String = _

    def path(path: String): TextSink[T] = {
        this.path = path
        this
    }

    override def save(): Unit = {
        writer.save(path)
    }

}
