package kosmag.events

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