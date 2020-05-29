package kosmag

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import kosmag.events.{AlertReactionEvent, BillingEvent}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.{CEP, PatternStream}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.util

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.windowing.time.Time
import scala.io.Source

import scala.reflect.io.File

object FlinkCEPExample {
  val rootDir: String = File(".").toAbsolute.toString()
  val boundedAssigner: AssignerWithPeriodicWatermarks[BillingEvent] = new AssignerWithPeriodicWatermarks[BillingEvent] {
    val maxOutOfOrderness = 360000L

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

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val billings: DataStream[BillingEvent] = env
      .readTextFile(f"$rootDir/src/test/resources/input-data.csv")
      .map(line => BillingEvent(line))
      .assignTimestampsAndWatermarks(boundedAssigner)

    val keyedBillings = billings
      .keyBy("id")

    val pattern = Pattern.begin[BillingEvent]("A", AfterMatchSkipStrategy.skipPastLastEvent())
      .where(new SimpleCondition[BillingEvent]() {
        override def filter(value: BillingEvent): Boolean = {
          value.balanceBefore >= 10 && value.balanceAfter < 10
        }
      })
      .next("B").oneOrMore().optional().where(new SimpleCondition[BillingEvent]() {
      override def filter(value: BillingEvent): Boolean = {
        value.balanceBefore >= value.balanceAfter
      }
    })
      .next("C").where(new SimpleCondition[BillingEvent]() {
      override def filter(value: BillingEvent): Boolean = {
        value.balanceBefore < value.balanceAfter
      }
    })
      .within(Time.hours(1))

    val patternStream: PatternStream[BillingEvent] = CEP.pattern(keyedBillings.javaStream, pattern)

    val result: SingleOutputStreamOperator[AlertReactionEvent] = patternStream.process(
      new PatternProcessFunction[BillingEvent, AlertReactionEvent]() {
        override def processMatch(
                                   patternMatch: util.Map[String, util.List[BillingEvent]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[AlertReactionEvent]): Unit = {
          out.collect(
            AlertReactionEvent(
              patternMatch.get("A").get(0).id,
              patternMatch.get("A").get(0).datetime,
              patternMatch.get("C").get(0).datetime
            )
          )
        }
      })

    result.print()

    tableEnv.execute("Flink CEP Example")
  }
}
