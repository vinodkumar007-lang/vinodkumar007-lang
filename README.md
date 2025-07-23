exstream/dev-SA/output/DEBTMAN/3d239002-e9e7-4d93-827f-8bd81c47f777
2025-07-23T21:11:05.489+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildProcessedFileEntries(SummaryJsonWriter.java:203) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildPayload(SummaryJsonWriter.java:78) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:223) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:110) ~[classes!/:na]
 at java.base/j
