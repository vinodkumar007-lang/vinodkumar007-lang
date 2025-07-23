2025-07-23T08:55:25.485+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-23T08:55:25.288+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
java.lang.NullPointerException: Cannot invoke "String.toUpperCase()" because "outputMethod" is null
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildProcessedFileEntries(SummaryJsonWriter.java:149) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildPayload(SummaryJsonWriter.java:78) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:223) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:110) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
 at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
