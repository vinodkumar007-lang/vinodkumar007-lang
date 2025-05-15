2025-05-15T19:08:57.434+02:00 ERROR 12464 --- [   scheduling-1] c.n.k.f.service.KafkaListenerService     : Error polling or processing Kafka record: Unrecognized token 'PublishEvent': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
 at [Source: (String)"PublishEvent(sourceSystem=DEBTMAN, timestamp=2025-05-12T09:06:07.843956600+02:00[Africa/Johannesburg], batchFiles=[BatchFile(objectId={1037A096-0000-CE1A-A484-3290CA7938C2}, repositoryId=BATCH, fileLocation=/path/to/file1, validationStatus=valid)], consumerReference=12345, processReference=check_process_reference, batchControlFileData=null)"; line: 1, column: 13]

com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'PublishEvent': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
 at [Source: (String)"PublishEvent(sourceSystem=DEBTMAN, timestamp=2025-05-12T09:06:07.843956600+02:00[Africa/Johannesburg], batchFiles=[BatchFile(objectId={1037A096-0000-CE1A-A484-3290CA7938C2}, repositoryId=BATCH, fileLocation=/path/to/file1, validationStatus=valid)], consumerReference=12345, processReference=check_process_reference, batchControlFileData=null)"; line: 1, column: 13]
	at com.fasterxml.jackson.core.JsonParser._constructError(JsonParser.java:2418) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportError(ParserMinimalBase.java:759) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser._reportInvalidToken(ReaderBasedJsonParser.java:3038) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser._handleOddValue(ReaderBasedJsonParser.java:2079) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser.nextToken(ReaderBasedJsonParser.java:805) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.ObjectMapper._readTreeAndClose(ObjectMapper.java:4759) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.ObjectMapper.readTree(ObjectMapper.java:3124) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.handleMessage(KafkaListenerService.java:74) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:54) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.scheduledFileProcessing(KafkaListenerService.java:69) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.scheduling.support.ScheduledMethodRunnable.run(ScheduledMethodRunnable.java:84) ~[spring-context-6.0.2.jar:6.0.2]
	at org.springframework.scheduling.support.DelegatingErrorHandlingRunnable.run(DelegatingErrorHandlingRunnable.java:54) ~[spring-context-6.0.2.jar:6.0.2]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.runAndReset(FutureTask.java:305) ~[na:na]
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:305) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:842) ~[na:na]

2025-05-15T19:08:57.459+02:00  INFO 12464 --- [   scheduling-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-05-15T19:08:57.519+02:00  INFO 12464 --- [   scheduling-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting generation and member id due to: consumer pro-actively leaving the group
2025-05-15T19:08:57.519+02:00  INFO 12464 --- [   scheduling-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-05-15T19:08:57.519+02:00  INFO 12464 --- [   scheduling-1] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-05-15T19:08:57.519+02:00  INFO 12464 --- [   scheduling-1] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-05-15T19:08:57.534+02:00  INFO 12464 --- [   scheduling-1] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-05-15T19:08:57.534+02:00  INFO 12464 --- [   scheduling-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for consumer-str-ecp-batch-1 unregistered
