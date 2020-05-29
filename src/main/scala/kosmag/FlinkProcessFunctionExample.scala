package kosmag

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util

import kosmag.events.{AlertReactionEvent, BillingEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.File

object FlinkProcessFunctionExample {
  val logger = LoggerFactory.getLogger(this.getClass)

  val withinValue = 3600000

  val rootDir: String = File(".").toAbsolute.toString()
  val boundedAssigner: AssignerWithPeriodicWatermarks[BillingEvent] = new AssignerWithPeriodicWatermarks[BillingEvent] {
    val maxOutOfOrderness = 0L

    var currentMaxTimestamp: Long = _

    override def extractTimestamp(element: BillingEvent, previousElementTimestamp: Long): Long = {
      val dateFormatter = DateTimeFormatter.ofPattern(element.dateFormat)
      val timestamp = LocalDateTime.parse(element.datetime, dateFormatter).toEpochSecond(ZoneOffset.ofHours(0)) * 1000
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }
  }

  case class KeyLastModified(key: String, lastModified: Long)

  class AlertReactionEventProcessor extends KeyedProcessFunction[Tuple, BillingEvent, AlertReactionEvent] {

    lazy val lastModifiedState: ValueState[KeyLastModified] = getRuntimeContext
      .getState(new ValueStateDescriptor[KeyLastModified]("last_modified_state", classOf[KeyLastModified]))

    lazy val alarmTriggerDatetime: ValueState[String] = getRuntimeContext
      .getState(new ValueStateDescriptor[String]("alarm_trigger_datetime", classOf[String]))

    lazy val alerted: ValueState[Boolean] = getRuntimeContext
      .getState(new ValueStateDescriptor[Boolean]("alerted", classOf[Boolean]))

    override def processElement(value: BillingEvent,
                                ctx: KeyedProcessFunction[Tuple, BillingEvent, AlertReactionEvent]#Context,
                                out: Collector[AlertReactionEvent]): Unit = {

      val current: KeyLastModified = lastModifiedState.value match {
        case null =>
          KeyLastModified(value.id, ctx.timestamp)
        case KeyLastModified(key, lastModified) =>
          KeyLastModified(key, ctx.timestamp)
      }
      logger.info(f"Event -> $value")

      if (value.balanceBefore >= 10 && value.balanceAfter < 10) {
        lastModifiedState.update(current)
        alerted.update(true)
        alarmTriggerDatetime.update(value.datetime)
        ctx.timerService.registerEventTimeTimer(current.lastModified + withinValue)
      }
      if (ctx.timestamp >= lastModifiedState.value.lastModified + withinValue) {
        logger.info(f"Time up for ${value.id} , ${alarmTriggerDatetime.value()}")
        alerted.update(false)
        alarmTriggerDatetime.update("")
      }
      if (value.balanceBefore < value.balanceAfter && alerted.value()) {
        lastModifiedState.update(current)

        out.collect(AlertReactionEvent(value.id, alarmTriggerDatetime.value(), value.datetime))

        alerted.update(false)
        alarmTriggerDatetime.update("")
      }
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = Source.fromFile(f"$rootDir/src/test/resources/input-data.csv")
    val billingData: Array[String] = source.getLines().toArray
    source.close

    val billings: DataStream[BillingEvent] = env
      .fromCollection(billingData)
      .map(line => BillingEvent(line))
      .assignTimestampsAndWatermarks(boundedAssigner)

    val result: DataStream[AlertReactionEvent] = billings
      .keyBy("id")
      .process(new AlertReactionEventProcessor)

    result.print()

    env.execute("Flink process function example")
  }
}
