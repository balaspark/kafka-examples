package com.zeyo.test.kinesis.kafka.custom.jackson

import org.apache.kafka.common.serialization.Serializer

import com.zeyo.test.kinesis.kafka.custom.model.User

import java.util
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature

object CustomSerializer {
  private val mapperInstance = new ObjectMapper() with ScalaObjectMapper
  mapperInstance.registerModule(DefaultScalaModule)
  mapperInstance.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  def mapper: ObjectMapper with ScalaObjectMapper = mapperInstance
}

class CustomUserSerializer extends Serializer[User] {

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
  }

  override def serialize(s: String, t: User): Array[Byte] = {
    CustomSerializer.mapper.writeValueAsBytes(t)
  }

  override def close(): Unit = {
  }

}