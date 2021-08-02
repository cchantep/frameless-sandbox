package cchantep.sandbox

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  var sparkSession: SparkSession = _

  def withSpark[T](f: SparkSession => T): T = {
    try {
      sparkSession = SparkSession.builder.master("local[2]").getOrCreate()
      f(sparkSession)
    } catch {
      case NonFatal(cause) => throw cause
    }
  }
}
