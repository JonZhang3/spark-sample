package com.jon.utils.connector.source

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrameReader

trait FileSource {

    val reader: DataFrameReader

    private val MODIFICATION_FORMAT = "YYYY-MM-DDTHH:mm:ss"
    private val DATE_FORMATER = new SimpleDateFormat(MODIFICATION_FORMAT)

    /**
     * 设置路径的全局过滤器，仅包含符合正则表达式规则的文件
     * 例： option("pathGlobFilter", "*.parquet")
     * json file should be filtered out
     *
     * @param patternString 正则表达式
     * @return
     */
    def pathGlobFilter(patternString: String): FileSource = {
        reader.option("pathGlobFilter", patternString)
        this
    }

    /**
     * 是否递归查找文件
     *
     * @param flag true 表示递归查找文件
     * @return
     */
    def recursiveFileLookup(flag: Boolean = true): FileSource = {
        reader.option("recursiveFileLookup", flag)
        this
    }

    def modifiedBefore(date: Date): FileSource = {
        reader.option("modifiedBefore", DATE_FORMATER.format(date))
        this
    }

    def modifiedAfter(date: Date): FileSource = {
        reader.option("modifiedAfter", DATE_FORMATER.format(date))
        this
    }

}
