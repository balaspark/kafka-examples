package com.zeyo.test.kinesis.kafka.custom.jackson

import org.apache.kafka.clients.producer.Partitioner

import java.util
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.utils.Utils
import com.zeyo.test.kinesis.kafka.custom.model.User

class CustomPartitioner extends Partitioner {

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    if (value.asInstanceOf[User].getName.startsWith("B") || value.asInstanceOf[User].getAge < 18)
      0
    else
      1
  }

  override def close(): Unit = {}
}