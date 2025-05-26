2025-05-26T09:31:53.935+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T09:31:53.937+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T09:31:53.937+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748244713933
2025-05-26T09:31:54.808+02:00  INFO 20332 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T09:31:54.814+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T09:31:54.919+02:00  INFO 20332 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T09:31:55.045+02:00  INFO 20332 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Partition 0: beginning offset = 18498, end offset = 18524
2025-05-26T09:31:55.062+02:00  INFO 20332 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Offset for 3 days ago in partition 0 = 18498
2025-05-26T09:31:55.062+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Seeking to offset 18498 for partition str-ecp-batch-composition-0
2025-05-26T09:31:55.063+02:00  INFO 20332 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Current position after seek on partition 0: 18498
2025-05-26T09:31:55.146+02:00  INFO 20332 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Polled 26 record(s) from Kafka
2025-05-26T09:31:55.174+02:00  WARN 20332 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Missing mandatory field 'BatchId'
2025-05-26T09:31:55.201+02:00  INFO 20332 --- [nio-8080-exec-2] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T09:31:55.211+02:00  INFO 20332 --- [nio-8080-exec-2] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
