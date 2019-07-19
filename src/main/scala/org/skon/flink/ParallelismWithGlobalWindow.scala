package org.skon.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object ParallelismWithGlobalWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(50L)
    env.setParallelism(1)

    val maxParallelism = 4
    val parallelismForTimestamp = 4

    val stream = env
      .addSource(new SourceFunction[(Long, Long, Long)] {
        override def run(ctx: SourceFunction.SourceContext[(Long, Long, Long)]): Unit = {
          (0 to 75000).foreach(count => ctx.collect((count, 1L, 2L)))
        }
        override def cancel(): Unit = {}
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, Long, Long)](Time.seconds(0L)) {
        override def extractTimestamp(element: (Long, Long, Long)): Long = element._1
      })

    stream
      .keyBy(_._2)
      .window(GlobalWindows.create)
      .evictor(TimeEvictor.of(Time.seconds(20L)))
      .trigger(CountTrigger.of(1L))
      .apply[(Long, Long, Long)]((_: Long, _: GlobalWindow, elements: Iterable[(Long, Long, Long)], out: Collector[(Long, Long, Long)]) => out.collect(elements.last))
      .setParallelism(maxParallelism)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, Long, Long)](Time.seconds(0L)) {
        override def extractTimestamp(element: (Long, Long, Long)): Long = element._1
      })
      .setParallelism(parallelismForTimestamp)
      .keyBy(_._3)
      .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
      .reduce((_, v2) => v2)
      .setParallelism(maxParallelism)
      .process[(Long, Long, Long)]((value, _, out) => {
        Console.println(value)
        out.collect(value)
      })
      .addSink(new CollectionSink[(Long, Long, Long)])

    env.execute("Parallelism Test with Global Window")
  }
}
