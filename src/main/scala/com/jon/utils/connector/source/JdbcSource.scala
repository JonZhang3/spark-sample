package com.jon.utils.connector.source

import java.time.Duration

import org.apache.spark.sql.DataFrameReader

class JdbcSource(val reader: DataFrameReader) extends Source(reader) {

    def url(url: String): JdbcSource = {
        reader.option("url", url)
        this
    }

    def dbtable(table: String): JdbcSource = {
        reader.option("dbtable", table)
        this
    }

    def query(queryString: String): JdbcSource = {
        reader.option("query", queryString)
        this
    }

    def driver(className: String): JdbcSource = {
        reader.option("driver", className)
        this
    }

    /**
     *
     * @param partitionColumn 分区字段，需要是数值类的
     * @param numPartitions 最大分区数量，必须为整数，当为0或负整数时，实际的分区数为1
     * @param lowerBound 下界，必须为整数
     * @param upperBound 上界，必须为整数
     */
    def partitionBy(partitionColumn: String, numPartitions: Long,
                    lowerBound: Long, upperBound: Long): JdbcSource = {
        reader.option("partitionColumn", partitionColumn)
        reader.option("numPartitions", numPartitions)
        reader.option("lowerBound", lowerBound)
        reader.option("upperBound", upperBound)
        this
    }

    def queryTimeout(time: Duration): JdbcSource = {
        reader.option("queryTimeout", time.getSeconds)
        this
    }

    // only read
    def fetchsize(size: Long): JdbcSource = {
        reader.option("fetchsize", size)
        this
    }



    def user(user: String): JdbcSource = {
        reader.option("user", user)
        this
    }

    def password(password: String): JdbcSource = {
        reader.option("password", password)
        this
    }

    override def load(): Unit = {
        reader.load()
    }

}
