2025-05-26T10:20:59.229+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T10:20:59.229+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T10:20:59.229+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748247659229
2025-05-26T10:20:59.285+02:00  INFO 4788 --- [nio-8080-exec-3] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T10:20:59.289+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T10:20:59.289+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Seeking to offset 18499 for partition str-ecp-batch-composition-0
2025-05-26T10:20:59.289+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Seeking partition 0 to offset 18499
2025-05-26T10:20:59.351+02:00  INFO 4788 --- [nio-8080-exec-3] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T10:20:59.436+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Polled 25 record(s) from Kafka
2025-05-26T10:20:59.436+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Processing record from topic-partition-offset str-ecp-batch-composition-0-18499: key='null'
2025-05-26T10:20:59.436+02:00  WARN 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Missing mandatory field 'BatchId'
2025-05-26T10:20:59.440+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T10:20:59.440+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Updated lastProcessedOffsets: {str-ecp-batch-composition-0=18499}
2025-05-26T10:20:59.440+02:00  INFO 4788 --- [nio-8080-exec-3] c.n.k.f.service.KafkaListenerService     : Final Response sent to topic: str-ecp-batch-composition-complete
2025-05-26T10:20:59.441+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting generation and member id due to: consumer pro-actively leaving the group
2025-05-26T10:20:59.441+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-05-26T10:20:59.441+02:00  INFO 4788 --- [nio-8080-exec-3] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-05-26T10:20:59.441+02:00  INFO 4788 --- [nio-8080-exec-3] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-05-26T10:20:59.442+02:00  INFO 4788 --- [nio-8080-exec-3] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-05-26T10:20:59.445+02:00  INFO 4788 --- [nio-8080-exec-3] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for consumer-str-ecp-batch-2 unregistered
