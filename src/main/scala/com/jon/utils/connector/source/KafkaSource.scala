package com.jon.utils.connector.source

import com.jon.utils.Connectors
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.streaming.DataStreamReader

class KafkaSource(val reader: DataFrameReader = _, val sreader: DataStreamReader = _)
    extends Source(reader, sreader) {

    if(reader != null) {
        reader.format(Connectors.FORMATER_KAFKA)
    } else {
        sreader.format(Connectors.FORMATER_KAFKA)
    }

    override type R = KafkaSource

    def bootstrapServers(hostAndPorts: String): KafkaSource = {
        option("kafka.bootstrap.servers", hostAndPorts)
    }

    def subscribe(subscribe: String): KafkaSource = {
        option("subscribe", subscribe)
    }

    override def load(): Unit = {
        if(reader != null) {
            reader.load()
        } else {
            sreader.load()
        }
    }

}
