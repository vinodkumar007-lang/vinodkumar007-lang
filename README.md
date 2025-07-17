0 to the committed offset FetchPosition{offset=18815, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[nsnxeteelpka03.nednet.co.za:9093 (id: 2 rack: null)], epoch=16}}
2025-07-17T12:59:25.190+02:00  INFO 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : str-ecp-batch: partitions assigned: [str-ecp-batch-composition-0]
2025-07-17T12:59:25.584+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📊 Total customerSummaries parsed: 39
2025-07-17T12:59:25.607+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
java.lang.NullPointerException: null
 at java.base/java.util.Objects.requireNonNull(Objects.java:209) ~[na:na]
 at java.base/java.util.ImmutableCollections.listFromTrustedArray(ImmutableCollections.java:213) ~[na:na]
 at java.base/java.util.List.of(List.java:866) ~[na:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:471) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:202) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:109) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
