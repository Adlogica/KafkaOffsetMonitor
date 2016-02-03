package com.quantifind.kafka.offsetapp.alerts

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.{OWArgs, OffsetInfoReporter}
import com.squareup.pagerduty.incidents.{PagerDuty, Trigger}
import kafka.utils.Logging

/**
  * Created by radu on 02/02/16.
  */
class PagerDutyOffsetInfoReporter(args: OWArgs) extends OffsetInfoReporter with Logging {

    private val pagerDuty = PagerDuty.create(args.pagerDutyKey)

    private val groupTopicMaxLagMap: Map[String, Long] = createGroupTopicMaxLagMap(args.groupTopicMaxLagConfigs)

    override def report(offsetInfoSeq: IndexedSeq[OffsetInfo]) = {
        offsetInfoSeq.foreach { offsetInfo =>
            groupTopicMaxLagMap.get(s"${offsetInfo.group}:${offsetInfo.topic}").foreach { maxLag =>
                if (offsetInfo.lag > maxLag) {
                    info(s"Max lag of ${maxLag} exceeded for Group = ${offsetInfo.group}, Topic = ${offsetInfo.topic}, Lag = ${offsetInfo.lag}")
                    triggerLagAlert(offsetInfo.group, offsetInfo.topic, offsetInfo.lag)
                }
            }
        }
    }

    private def triggerLagAlert(consumerGroup: String, topic: String, lag: Long) = {
        val trigger = new Trigger.Builder(s"${consumerGroup} - ${topic} lag is $lag")
            .withIncidentKey(s"$consumerGroup-$topic")
            .addDetails("Consumer Group", consumerGroup)
            .addDetails("Topic", topic)
            .addDetails("Lag", lag.toString)
            .build
        pagerDuty.notify(trigger)
    }

    private def createGroupTopicMaxLagMap(configs: String): Map[String, Long] = {
        Map[String, Long]() ++ configs.split(",")
            .map { entry => entry.split("=") }
            .map { array => (array(0), array(1).toLong) }
    }
}
