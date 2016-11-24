package org.iancsmith.sparktestapp1

/* SimpleApp.scala */
import org.apache.spark.{SparkConf, SparkContext}

object SparkTestApp1 {
  def main(args: Array[String]) {
    val logFile = "/home/ian/sparktestapp1.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("SparkTestApp1")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
