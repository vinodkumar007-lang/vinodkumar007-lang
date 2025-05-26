2025-05-26T10:30:48.257+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T10:30:48.263+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T10:30:48.263+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748248248254
2025-05-26T10:30:49.127+02:00  INFO 10524 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T10:30:49.133+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T10:30:49.224+02:00  INFO 10524 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T10:30:49.308+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Seeking to offset 18498 for partition str-ecp-batch-composition-0
2025-05-26T10:30:49.310+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Seeking partition 0 to offset from 10 days ago: 18498
2025-05-26T10:30:49.409+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Polled 26 record(s) from Kafka
2025-05-26T10:30:49.410+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Processing record from topic-partition-offset str-ecp-batch-composition-0-18498: key='null'
2025-05-26T10:30:49.430+02:00  WARN 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Missing mandatory field 'BatchId'
2025-05-26T10:30:49.475+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T10:30:49.475+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Updated lastProcessedOffsets: {str-ecp-batch-composition-0=18498}
2025-05-26T10:30:49.486+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
