package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import io.delta.tables._

object DataPrep {
  def main(args: Array[String]): Unit = {
    val arg_s3_ip: String = sys.env.getOrElse("ARG_S3_IP", "default_value")
    val arg_s3_port: String = sys.env.getOrElse("ARG_S3_PORT", "default_value")
    val arg_s3_access: String = sys.env.getOrElse("ARG_S3_ACCESS", "default_value")
    val arg_s3_secret: String = sys.env.getOrElse("ARG_S3_SECRET", "default_value")

    lazy val spark = SparkSession.builder.appName("DataPrep")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()

    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark.conf.set("spark.sql.shuffle.partitions", 1)

    val connectionTimeOut = "600000"
    val s3endPointLoc: String = s"http://${arg_s3_ip}:${arg_s3_port}"



    spark.conf.set("fs.s3a.endpoint", s3endPointLoc)
    spark.conf.set("fs.s3a.access.key", arg_s3_access)
    spark.conf.set("fs.s3a.secret.key", arg_s3_secret)
    spark.conf.set("fs.s3a.connection.timeout", connectionTimeOut)

    spark.conf.set("spark.sql.debug.maxToStringFields", "100")
    spark.conf.set("fs.s3a.path.style.access", "true")
    spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("fs.s3a.connection.ssl.enabled", "false")

    val sourceBucket: String = "test"
    //val inputPath: String = s"s3a://$sourceBucket/transactions/Detail0511.csv"
    val file_name: String = "Detail0512"
    val inputPath: String = s"s3a://$sourceBucket/transactions/$file_name.csv"
    val outputPath: String = s"s3a://$sourceBucket/transactions_stg/$file_name"


    val df = spark.read.format("csv")
      .options(Map("delimiter" -> ",", "header" -> "true"))
      .load(inputPath)

    //df.printSchema()
    df.show(1)

    df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .option("path", outputPath)
      .saveAsTable(file_name)
    spark.stop()
  }
}
