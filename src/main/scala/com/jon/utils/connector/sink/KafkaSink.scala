package com.jon.utils.connector.sink

import com.jon.utils.Connectors
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.DataStreamWriter

class KafkaSink[T](val writer: DataFrameWriter[T] = _, val swriter: DataStreamWriter[T] = _)
    extends Sink[T](writer, swriter) {

    if(writer != null) {
        writer.format(Connectors.FORMATER_KAFKA)
    } else {
        swriter.format(Connectors.FORMATER_KAFKA)
    }

    override type R = KafkaSink[T]

    def bootstrapServers(hostAndPorts: String): KafkaSink[T] = {
        option("kafka.bootstrap.servers", hostAndPorts)
    }

    def topic(topic: String): KafkaSink[T] = {
        option("topic", topic)
    }

    override def save(): Unit = {
        writer.save()
    }

}
