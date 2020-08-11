package com.zeyo.test.kinesis.kafka.custom.jackson

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import com.zeyo.test.kinesis.kafka.custom.model.User
import org.apache.kafka.clients.producer.ProducerRecord

object KakfaUserProducer extends App {

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "com.zeyo.test.kinesis.kafka.custom.jackson.CustomUserSerializer")
  props.put("partitioner.class", "com.zeyo.test.kinesis.kafka.custom.jackson.CustomPartitioner")
  val producer = new KafkaProducer[String, User](props)
  try {
    for (i <- 10 to 20) {

      val user = func(i)

      val record = new ProducerRecord[String, User]("custompartitioner", i.toString, user)
      val metadata = producer.send(record)
      printf(
        s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(), metadata.get().partition(),
        metadata.get().offset());
    }

  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }

  def func(i: Int): User = if (i % 2 == 0) new User("Bala - " + i, i)
  else new User("User - " + i, i)
}