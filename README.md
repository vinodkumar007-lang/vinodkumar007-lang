0=OffsetAndMetadata{offset=19244, leaderEpoch=null, metadata=''}}
2025-09-10T06:15:43.239+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : 🚀 [batchId: 820b43b9-6584-43ab-95a4-65930fde6143] Calling Orchestration API: ${OT_SERVICE_MFC_URL}
2025-09-10T06:15:43.239+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : 📡 Initiating OT orchestration call to URL: ${OT_SERVICE_MFC_URL} for batchId: 820b43b9-6584-43ab-95a4-65930fde6143 and sourceSystem: MFC
2025-09-10T06:15:43.240+02:00 ERROR 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : ❌ Exception during OT orchestration call for batchId: 820b43b9-6584-43ab-95a4-65930fde6143 - Not enough variable values available to expand 'OT_SERVICE_MFC_URL'
java.lang.IllegalArgumentException: Not enough variable values available to expand 'OT_SERVICE_MFC_URL'
 at org.springframework.web.util.UriComponents$VarArgsTemplateVariables.getValue(UriComponents.java:370) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.UriComponents.expandUriComponent(UriComponents.java:263) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.HierarchicalUriComponents$PathSegmentComponent.expand(HierarchicalUriComponents.java:993) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.HierarchicalUriComponents.expandInternal(HierarchicalUriComponents.java:439) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.HierarchicalUriComponents.expandInternal(HierarchicalUriComponents.java:52) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.UriComponents.expand(UriComponents.java:172) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.DefaultUriBuilderFactory$DefaultUriBuilder.build(DefaultUriBuilderFactory.java:403) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.util.DefaultUriBuilderFactory.expand(DefaultUriBuilderFactory.java:154) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.execute(RestTemplate.java:763) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.exchange(RestTemplate.java:646) ~[spring-web-6.0.2.jar!/:6.0.2]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.callOrchestrationBatchApi(KafkaListenerService.java:452) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:196) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
 at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
2025-09-10T06:15:43.249+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : 📤 [batchId: 820b43b9-6584-43ab-95a4-65930fde6143] OT request sent successfully
2025-09-10T06:15:43.249+02:00  INFO 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [820b43b9-6584-43ab-95a4-65930fde6143] ⏳ Waiting for XML for jobId=null, id=null
2025-09-10T06:15:43.249+02:00 ERROR 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [820b43b9-6584-43ab-95a4-65930fde6143] ❌ Error post-OT summary generation: Cannot invoke "String.isEmpty()" because "segment" is null
java.lang.NullPointerException: Cannot invoke "String.isEmpty()" because "segment" is null
 at java.base/sun.nio.fs.UnixFileSystem.getPath(UnixFileSystem.java:271) ~[na:na]
 at java.base/java.nio.file.Path.of(Path.java:147) ~[na:na]
 at java.base/java.nio.file.Paths.get(Paths.java:69) ~[na:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.waitForXmlFile(KafkaListenerService.java:492) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:306) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:198) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExec
