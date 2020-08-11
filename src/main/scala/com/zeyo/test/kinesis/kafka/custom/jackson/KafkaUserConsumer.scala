package com.zeyo.test.kinesis.kafka.custom.jackson

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import com.zeyo.test.kinesis.kafka.custom.model.User
import scala.collection.JavaConverters._

object KafkaUserConsumer extends App {

  val prop: Properties = new Properties()
  prop.put("group.id", "custompartitioncheck")
  prop.put("bootstrap.servers", "localhost:9092")
  prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.put("value.deserializer", "com.zeyo.test.kinesis.kafka.custom.jackson.CustomUserDeSerializer")
  prop.put("enable.auto.commit", "true")
  prop.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer[String, User](prop)
  val topics = List("custompartitioner")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value().getName +
          ", Offset: " + record.offset() + ", Partition: " + record.partition())
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }

}