
# My experience with Apache Flink for Complex Event Processing
### Kosma Grochowski
My goal is to create a comprehensive review of available options when dealing with Complex Event Processing using Apache Flink. We will be building a simple proof-of-concept solution for an example use case.

The codebase for examples is provided at [GitHub](https://github.com/kosmag/flink-cep-examples).
## Overview
### Complex Event Processing (CEP)
The term "complex event processing" defines methods of analyzing pattern relationships between streamed events. When done in real-time, it can provide advanced insights further into the data processing system.

There are numerous industries in which complex event processing has found widespread use, financial sector, IoT and Telco to name a few. Those uses include real-time marketing, fraud and anomalies detection and a variety of other business automation cases. 

### Flink
Flink is a promising framework to combat the subject of complex event processing. It supports low-latency stream processing. I recommend [Flink docs](https://ci.apache.org/projects/flink/flink-docs-stable/) as the best way to learn more about the project - they're very well written and cover both high-level concepts and concrete API calls.

Complex events may be processed in Flink using several different approaches, three of which I'll cover moving forwards:

* [Process Function approach](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html)
* [FlinkCEP approach](https://ci.apache.org/projects/flink/flink-docs-stable/dev/libs/cep.html)
* [Flink SQL MATCH_RECOGNIZE approach](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/match_recognize.html)

I'll be using Scala as a programming language of choice.

## Use case
I believe that the best way to learn the strengths and weaknesses of each approach is to show them in action. Therefore, I created an example use case inspired by the real business needs of one of our Telco clients. We had to solve this case on the scale of hundreds of thousands of events per second and over ten million unique subscribers.
### Description
Let's say a telco company would like to make use of its billing data to make advertising decisions to its pre-paid customers. The billing data structure would be as follows:


*  client id (presumably his phone number)
*  event timestamp
*  client's pre-paid balance before the event
*  client's pre-paid balance after the event

The company currently sends alert information to those customers, whose balance approaches 0\$. The threshold under which alarm will trigger is set at 10\$. The company would like to know customers' reactions to the alarm trigger. It estimates that if the customer tops up his account within one hour from the alarm trigger, this is the reaction sparked by that alarm.

Thus, the event pattern we would like to capture would look like this:

* event with balance before value >= 10\$ and balance after value < 10\$ (alarm trigger)
* zero or more events with balance after <= balance before (non-top-up actions)
* event with balance after > balance before (top-up action), happening within one hour from the first event

So, we can define input and output structures like this:

    case class BillingEvent(id: String,
                            datetime: String,
                            balanceBefore: Long,
                            balanceAfter: Long) {
      val dateFormat = "yyyy-MM-dd HH:mm:ss"
    }
    
    object BillingEvent {
      def apply(line: String): BillingEvent = {
        val splitLine: Array[String] = line.split(",")
        BillingEvent(
          splitLine(0),
          splitLine(1),
          splitLine(2).toLong,
          splitLine(3).toLong
        )
      }
    
    }



    case class AlertReactionEvent(id: String,
                              alarmTriggerDatetime: String,
                              topupDatetime: String)


### ProcessFunction solution
Stateful stream processing is the lowest level of abstraction provided in Flink API. Because of it, it is suitable for the custom tasks not covered by higher-level APIs. An example solution using `ProcessFunction` will look as follows:

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

      if (value.balanceBefore >= 10 && value.balanceAfter < 10) {
        lastModifiedState.update(current)
        alerted.update(true)
        alarmTriggerDatetime.update(value.datetime)
        ctx.timerService.registerEventTimeTimer(current.lastModified + withinValue)
      }
      if(ctx.timestamp >= lastModifiedState.value.lastModified + withinValue){
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



As we can see, Flinks `ProcessFunction` API is a powerful tool that can accurately capture patterns of any complexity. However, heavy usage of state variables is needed. Moreover, in the real world applications of stream processing, we have to deal with out-of-order events and this API level would then require us to keep track of this order, using for example a priority queue. The following solutions will provide a more streamlined way of thinking about complex patterns, resulting in better clarity, as well as built-in sorting as support for out-of-order events.

### FlinkCEP solution
FlinkCEP is the library that allows complex patterns definition using its higher level Pattern API. Internally, the pattern is abstracted to a non-deterministic finite automaton. We can, therefore, draw similarities between event pattern matching and regular expression matching. 

To illustrate, let's define alarm trigger event as event A, following zero or more non top up actions as event B and finally top up action as C. Then, the abstracted target event pattern will be `A B* C`. FlinkCEP equivalent will look roughly like this:

    Pattern
        .begin("A").where(/* conditions */)
        .next("B").oneOrMore().optional().where(/* conditions */)
        .next("C").where(/* conditions */)




Matched patterns can be processed with `PatternProcessedFunction`, which is `ProcessFunction` equivalent in Pattern API, taking whole matched event chains as input.

FlinkCEP implementation is shown below:

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


Flink CEP strength is an intuitive method of modeling event cause-and-effect relations. With interface more suitable for the job than `ProcessFunction` one, FlinkCEP-powered applications will be easier to maintain and scale.


### Flink SQL MATCH_RECOGNIZE solution
In December 2016, SQL standard [(link)](https://standards.iso.org/ittf/PubliclyAvailableStandards/c065143_ISO_IEC_TR_19075-5_2016.zip) was enriched with MATCH\_RECOGNIZE clause to make pattern recognition with SQL possible. Flink support for MATCH\_RECOGNIZE clause was added in version 1.7, following issue [FLIP-20](https://cwiki.apache.org/confluence/display/FLINK/FLIP-20%3A+Integration+of+SQL+and+CEP). Under the hood, MATCH\_RECOGNIZE is implemented using Flink CEP. We will codify our event phases exactly as in the FlinkCEP section.


Declarative way of approaching our problem results in the following code:


```
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

```

SQL option provides arguably the best clarity and brevity of them all. Moreover, this approach allows for better mutual understanding between data engineers and data analysts, as SQL is commonly used by both groups.

## It's not all roses, though

While MATCH\_RECOGNIZE approach may look universally optimal, there are still some problems due to the freshness of the project.

### What Flink SQL MATCH_RECOGNIZE can't do (yet)
Flink still lacks full support for MATCH_RECOGNIZE standard. [Here](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/match_recognize.html#known-limitations) is the list of some of the missing features. Depending on your use case, this may end up not being the problem or it may require working your way out of the unsupported clause with additional code. Hopefully, over time those holes in the standard will be plugged by the Flink development team.

### What the standard itself can't do (yet?)
While holes may eventually be plugged in Flink implementation, it may still not be enough to cover all CEP use cases in the current version of the MATCH_RECOGNIZE standard.
Notably, as it stands, the standard lacks a dedicated way to deal with time constraints. 

Currently, Flink implementation handles simple time constraints with the use of aforementioned non-standard WITHIN clause which can be helpful for a lot of cases. However, there are still some gaps. 


### Unruly case
In the case we implemented for our client, the requirement was to also detect the absence of the event. And that appeared to be impossible using MATCH\_RECOGNIZE clause.

Let's say that our company, in addition to "top-up within one hour" events would like to track those customers who do not top-up in the appointed time. Therefore, after an hour without a top-up, we would like to generate an appropriate event. Unfortunately, there is no capability in the current SQL standard nor in Flink SQL implementation to define how to handle such "absence of the top-up event in one hour" cases.

A way out, which seems most in line with MATCH\_RECOGNIZE event handling, would be to implement the pattern using FlinkCEP, which in addition to `within` functionality provides `processTimedOutMatch` function, which would allow passing timed out events to side output. 

    val patternStream: PatternStream[BillingEvent] = CEP.pattern(keyedBillings.javaStream, pattern)

    val sideOutputTag = OutputTag[AlertReactionEvent]("absence-of-topup-event")
    val result: SingleOutputStreamOperator[AlertReactionEvent] = patternStream.process(
      new PatternProcessFunction[BillingEvent, AlertReactionEvent]() with TimedOutPartialMatchHandler[BillingEvent] {
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

        override def processTimedOutMatch(patternMatch: util.Map[String, util.List[BillingEvent]],
                                          ctx: PatternProcessFunction.Context): Unit = {
          ctx.output(
            sideOutputTag,
            AlertReactionEvent(
              patternMatch.get("A").get(0).id,
              patternMatch.get("A").get(0).datetime,
              ""
            )
          )
        }
      })

    result.getSideOutput(sideOutputTag).print()



## Conclusions
Flink provides a variety of ways of handling complex event processing. Each way has its merit:

*  `FlinkCEP` is the more versatile approach
*  `Flink SQL MATCH_RECOGNIZE` is the more expressive one
*  `ProcessFunction` is an everything-goes backup for highly non-standard transformations. 

Which one is the best changes according to the specific use case. We hope that someday most, if not all, of complex pattern matching will be possible in a higher-level language like Flink SQL as this will enable analysts to define business rules themselves, without low-level coding, resulting in increased productivity.


Finally, I'd like to thank Krzysztof Zarzycki and a whole [GetInData](https://getindata.com) team for their invaluable support - none of it would have been possible without their help :)