package org.skon.flink

import org.apache.flink.streaming.api.functions.sink.SinkFunction

class CollectionSink[T] extends SinkFunction[T] {
  private val values: List[String] = List[String]()

  override def invoke(value: T): Unit = values + value.toString
}
