2025-05-26T09:14:28.225+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T09:14:28.231+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T09:14:28.231+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748243668223
2025-05-26T09:14:29.017+02:00  INFO 5144 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T09:14:29.023+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T09:14:29.138+02:00  INFO 5144 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T09:14:29.199+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Partition 0: beginning offset = 18498, end offset = 18524
2025-05-26T09:14:29.213+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Offset for 3 days ago in partition 0 = 18517
2025-05-26T09:14:29.213+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Seeking to offset 18517 for partition str-ecp-batch-composition-0
2025-05-26T09:14:29.215+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Current position after seek on partition 0: 18517
2025-05-26T09:14:29.282+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Polled 7 record(s) from Kafka
2025-05-26T09:14:30.325+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : ✅ Uploaded 'DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_761545f6-8a05-417f-94d6-5c26c478d8f5.csv'
2025-05-26T09:14:30.364+02:00  INFO 5144 --- [nio-8080-exec-2] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T09:14:30.378+02:00  INFO 5144 --- [nio-8080-exec-2] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
