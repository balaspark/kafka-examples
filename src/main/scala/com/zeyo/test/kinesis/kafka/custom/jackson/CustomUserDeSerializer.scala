package com.zeyo.test.kinesis.kafka.custom.jackson

import org.apache.kafka.common.serialization.Deserializer

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.zeyo.test.kinesis.kafka.custom.model.User
import java.util

object CustomDeSerializer {
  private val mapperInstance = new ObjectMapper() with ScalaObjectMapper
  mapperInstance.registerModule(DefaultScalaModule)
  mapperInstance.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  def mapper: ObjectMapper with ScalaObjectMapper = mapperInstance
}

class CustomUserDeSerializer extends Deserializer[User] {

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def deserialize(s: String, bytes: Array[Byte]): User = {
    CustomDeSerializer.mapper.readValue(bytes, classOf[User])
  }

  override def close(): Unit = {
  }
}