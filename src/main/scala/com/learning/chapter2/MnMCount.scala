package com.learning.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MnMCount").getOrCreate();

    if (args.length < 1) {
      println("Usage: MnMCount <mnm_file_dataset>")
      return
    }

    val mnmFile = args(0)
    val mnmDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    val countMnmDf = mnmDf.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnmDf.show(60)

    println(s"Total Rows = ${countMnmDf.count()}")
    println()

    val californiaMnmDf = mnmDf.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    californiaMnmDf.show(10)

    System.in.read()
    spark.stop()
  }
}
