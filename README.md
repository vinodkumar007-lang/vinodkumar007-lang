2025-05-26T12:03:30.423+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T12:03:30.426+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T12:03:30.426+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748253810418
2025-05-26T12:03:31.276+02:00  INFO 21240 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T12:03:31.284+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T12:03:31.345+02:00  INFO 21240 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T12:03:31.360+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Seeking to offset 18498 for partition str-ecp-batch-composition-0
2025-05-26T12:03:31.362+02:00  INFO 21240 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Seeking partition 0 to offset from 10 days ago: 18498
2025-05-26T12:03:31.455+02:00  INFO 21240 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Polled 33 record(s) from Kafka
2025-05-26T12:03:31.457+02:00  INFO 21240 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Processing record from topic-partition-offset str-ecp-batch-composition-0-18498: key='null'
2025-05-26T12:03:31.482+02:00  WARN 21240 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Missing mandatory field 'BatchId'
2025-05-26T12:03:31.539+02:00  INFO 21240 --- [nio-8080-exec-2] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T12:03:31.539+02:00  INFO 21240 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Updated lastProcessedOffsets: {str-ecp-batch-composition-0=18498}
2025-05-26T12:03:31.557+02:00  INFO 21240 --- [nio-8080-exec-2] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
