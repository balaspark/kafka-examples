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

object Task1ReadingNifiWritingCassandra {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafka-streaming").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(conf, Seconds(2))

    val topics = Array("test_iniyavel")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "nifi2cassandara", "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val streamdata = stream.map(x => (x.value))

    streamdata.foreachRDD(x => {

      if (!x.isEmpty()) {
        println("received data from KafkaMQ")

        val userdf = spark.read.json(x)
          .withColumn("results", explode(col("results")))
          .select(
            "results.user.gender",
            "results.user.email", "results.user.username", 
            "nationality")
          .withColumn("rusername", regexp_replace(col("username"), "([0-9])", ""))
          .drop("username")
          .withColumn("id", monotonically_increasing_id() )

        userdf.show()
        
        userdf.write.format("org.apache.spark.sql.cassandra").option("keyspace", "zeyo_keys").option("table", "random_users").mode("append").save()
        
        println("written to Cassandara completed..")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}