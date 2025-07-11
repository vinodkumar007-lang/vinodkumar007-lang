2025-07-11T15:10:30.574+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📂 docgen folder not found. Retrying in 5000ms...
2025-07-11T15:10:35.768+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/6d5a9c27-595f-453d-868d-36262bd92c64/1a597464-fab1-46f2-8dab-56e952f2a81f/docgen/3dbe63e8-5c5f-4f1d-893e-5e0b992b9c03/output/_STDDELIVERYFILE.xml
2025-07-11T15:10:36.939+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-0000000000009882'
2025-07-11T15:10:37.250+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-0000000000009883'
2025-07-11T15:10:37.558+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-000000000000988B'
2025-07-11T15:10:37.941+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-000000000000988E'
2025-07-11T15:10:38.240+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-000000000000988F'
2025-07-11T15:10:38.641+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-0000000000009890'
2025-07-11T15:10:38.741+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Error uploading file: /mnt/nfs/dev-exstream/dev-SA/output/DEBTMAN/6d5a9c27-595f-453d-868d-36262bd92c64/email/3768000010607501_CCEML805.pdf

java.nio.charset.MalformedInputException: Input length = 3
	at java.base/java.lang.String.throwMalformed(String.java:1242) ~[na:na]
	at java.base/java.lang.String.decodeUTF8_UTF16(String.java:1129) ~[na:na]
	at java.base/java.lang.String.newStringUTF8NoRepl(String.java:732) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl1(String.java:760) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl(String.java:742) ~[na:na]
	at java.base/java.lang.System$2.newStringNoRepl(System.java:2394) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3369) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3325) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$buildAndUploadProcessedFiles$5(KafkaListenerService.java:282) ~[classes!/:na]
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:133) ~[na:na]
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1845) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:762) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildAndUploadProcessedFiles(KafkaListenerService.java:267) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:115) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:67) ~[classes!/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
