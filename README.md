2025-07-18T06:18:58.187+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-18T06:18:59.845+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found stable XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/9054915a-facf-4402-b8ab-2ffb6a2a3037/4765fbe2-5260-4304-8a9f-c27853584f6a/docgen/029ba6f4-fa35-4f4d-8f4a-37ef30c35b5b/output/_STDDELIVERYFILE.xml
2025-07-18T06:18:59.846+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/9054915a-facf-4402-b8ab-2ffb6a2a3037/4765fbe2-5260-4304-8a9f-c27853584f6a/docgen/029ba6f4-fa35-4f4d-8f4a-37ef30c35b5b/output/_STDDELIVERYFILE.xml
2025-07-18T06:18:59.863+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🧾 Parsed error report with 0 entries
2025-07-18T06:19:00.793+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📊 Total customerSummaries parsed: 39
2025-07-18T06:19:00.984+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error uploading file: Input length = 3

java.nio.charset.MalformedInputException: Input length = 3
	at java.base/java.lang.String.throwMalformed(String.java:1242) ~[na:na]
	at java.base/java.lang.String.decodeUTF8_UTF16(String.java:1129) ~[na:na]
	at java.base/java.lang.String.newStringUTF8NoRepl(String.java:732) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl1(String.java:760) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl(String.java:742) ~[na:na]
	at java.base/java.lang.System$2.newStringNoRepl(System.java:2394) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3369) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3325) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:426) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:206) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:109) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-07-18T06:19:01.084+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error uploading file: Input length = 3

java.nio.charset.MalformedInputException: Input length = 3
	at java.base/java.lang.String.throwMalformed(String.java:1242) ~[na:na]
	at java.base/java.lang.String.decodeUTF8_UTF16(String.java:1129) ~[na:na]
	at java.base/java.lang.String.newStringUTF8NoRepl(String.java:732) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl1(String.java:760) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl(String.java:742) ~[na:na]
	at java.base/java.lang.System$2.newStringNoRepl(System.java:2394) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3369) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3325) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:426) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:206) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:109) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
