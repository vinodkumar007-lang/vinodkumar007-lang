2025-05-09T15:56:03.027+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-05-09T15:56:03.038+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-05-09T15:56:03.039+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-05-09T15:56:03.039+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-05-09T15:56:03.046+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for consumer-str-ecp-batch-1 unregistered
2025-05-09T15:56:03.047+02:00  INFO 17500 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : str-ecp-batch: Consumer stopped
2025-05-09T15:56:03.078+02:00  INFO 17500 --- [ionShutdownHook] o.s.i.endpoint.EventDrivenConsumer       : Removing {logging-channel-adapter:_org.springframework.integration.errorLogger} as a subscriber to the 'errorChannel' channel
2025-05-09T15:56:03.079+02:00  INFO 17500 --- [ionShutdownHook] o.s.i.channel.PublishSubscribeChannel    : Channel 'application.errorChannel' has 0 subscriber(s).
2025-05-09T15:56:03.079+02:00  INFO 17500 --- [ionShutdownHook] o.s.i.endpoint.EventDrivenConsumer       : stopped bean '_org.springframework.integration.errorLogger'
