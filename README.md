2025-05-26T10:30:49.502+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-05-26T10:30:49.585+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T10:30:49.586+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T10:30:49.586+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748248249585
2025-05-26T10:30:49.588+02:00 DEBUG 10524 --- [nio-8080-exec-1] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@1c16904f]
2025-05-26T10:30:49.663+02:00  INFO 10524 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition str-ecp-batch-composition-complete-0 to 30 since the associated topicId changed from null to kFgHvXbjT6KB5Z2coZAcJw
2025-05-26T10:30:49.664+02:00  INFO 10524 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T10:30:49.666+02:00  INFO 10524 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 194089 with epoch 0
2025-05-26T10:30:49.677+02:00  INFO 10524 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Final Response sent to topic: str-ecp-batch-composition-complete
2025-05-26T10:30:49.687+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-05-26T10:30:49.738+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting generation and member id due to: consumer pro-actively leaving the group
2025-05-26T10:30:49.738+02:00  INFO 10524 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-05-26T10:30:49.738+02:00  INFO 10524 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-05-26T10:30:49.738+02:00  INFO 10524 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-05-26T10:30:49.738+02:00  INFO 10524 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-05-26T10:30:49.751+02:00  INF
