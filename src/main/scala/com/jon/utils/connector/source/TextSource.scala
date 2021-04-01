package com.jon.utils.connector.source

import org.apache.spark.sql.DataFrameReader

class TextSource(val r: DataFrameReader)
    extends Source(r) with FileSource {

    override val reader: DataFrameReader = r

    private var paths: Seq[String] = _

    def paths(paths: String*): TextSource = {
        this.paths = paths
        this
    }

    override def load(): Unit = {
        r.load(this.paths: _*)
    }

}
