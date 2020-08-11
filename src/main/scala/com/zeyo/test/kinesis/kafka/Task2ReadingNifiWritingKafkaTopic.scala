package com.zeyo.test.kinesis.kafka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.spark_project.jetty.util.Callback.Nested
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode

object Task2ReadingNifiWritingKafkaTopic {

  case class UserName(username: String)

  case class User(user: UserName)

  case class Users(results: Array[User])

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafka-streaming").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    import org.apache.spark.sql.Encoders
    val jsonSchema = Encoders.product[Users].schema

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test_iniyavel")
      .option("startingOffsets", "earliest")
      .load()
      .select(col("value") cast "string" as "res")
      .select(from_json(col("res"), jsonSchema).as("jsonobj"))
      .withColumn("out", explode(col("jsonobj.results")))
      .select("out.user.username")

    df.printSchema()

    val query = df.map(x => x.get(0).toString().getBytes).toDF("value")
      .selectExpr("value as key", "value as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "kafka_test")
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", "checkpointLocation-kafka2console1") 
      .queryName("kafkaStream")
      .start()

    query.awaitTermination()

  }
}