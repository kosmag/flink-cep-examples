package kosmag.events

case class AlertReactionEvent(id: String,
                              alarmTriggerDatetime: String,
                              topupDatetime: String)

