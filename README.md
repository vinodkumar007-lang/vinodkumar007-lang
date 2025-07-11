2025-07-11T17:15:53.923+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/b3140c84-21d6-4663-91f4-66a19715626e/3ec8cc7f-89e0-4dbe-b8c5-70be5da0ef52/docgen/3625e826-79c5-41f9-883f-f0d6aad249ba/output/_STDDELIVERYFILE.xml
2025-07-11T17:15:54.639+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098E2'
2025-07-11T17:15:55.038+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098EB'
2025-07-11T17:15:55.459+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098EC'
2025-07-11T17:15:56.040+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098ED'
2025-07-11T17:15:56.438+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098EF'
2025-07-11T17:15:56.938+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Farchive%2FD69E7661F52C8DF-00000000000098F1'
2025-07-11T17:15:57.042+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Error uploading file: /mnt/nfs/dev-exstream/dev-SA/output/DEBTMAN/b3140c84-21d6-4663-91f4-66a19715626e/email/3768000010607501_CCEML805.pdf

java.nio.charset.MalformedInputException: Input length = 3
	at java.base/java.lang.String.throwMalformed(String.java:1242) ~[na:na]
	at java.base/java.lang.String.decodeUTF8_UTF16(String.java:1129) ~[na:na]
	at java.base/java.lang.String.newStringUTF8NoRepl(String.java:732) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl1(String.java:760) ~[na:na]
	at java.base/java.lang.String.newStringNoRepl(String.java:742) ~[na:na]
	at java.base/java.lang.System$2.newStringNoRepl(System.java:2394) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3369) ~[na:na]
	at java.base/java.nio.file.Files.readString(Files.java:3325) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$buildAndUploadProcessedFiles$7(KafkaListenerService.java:304) ~[classes!/:na]
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:133) ~[na:na]
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1845) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:762) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildAndUploadProcessedFiles(KafkaListenerService.java:289) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:114) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:67) ~[classes!/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169)
