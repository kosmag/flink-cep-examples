package kosmag

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import kosmag.events.{AlertReactionEvent, BillingEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

import scala.reflect.io.File

object FlinkSqlMatchRecognizeExample {
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

    val billingsTable: Table = tableEnv.fromDataStream(billings, 'id, 'datetime, 'balanceBefore, 'balanceAfter, 'user_action_time.rowtime)

    val result: Table = tableEnv.sqlQuery(
      f"""
         | SELECT *
         | FROM $billingsTable
         |   MATCH_RECOGNIZE (
         |     PARTITION BY id
         |     ORDER BY user_action_time
         |     MEASURES
         |       A.datetime AS alarm_trigger_datetime,
         |       C.datetime AS topup_datetime
         |     ONE ROW PER MATCH
         |     AFTER MATCH SKIP PAST LAST ROW
         |     PATTERN (A B* C) WITHIN INTERVAL '1' HOUR
         |     DEFINE
         |       A AS A.balanceBefore >= 10 AND A.balanceAfter < 10,
         |       B AS B.balanceBefore >= B.balanceAfter,
         |       C AS C.balanceBefore < C.balanceAfter
         |   ) t
         |""".stripMargin)

    val resultStream = tableEnv.toAppendStream[AlertReactionEvent](result)
    resultStream.print()

    tableEnv.execute("Flink SQL MATCH RECOGNIZE example")
  }
}
