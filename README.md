2025-08-14T16:18:54.629+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-08-14T16:18:58.529+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📦 Processed 85797 customer records
2025-08-14T16:18:58.529+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : ℹ️ No 'print' directory found in jobDir: /mnt/nfs/ete-exstream/ete-SA/output/MFC/010dd1e9-4c43-4fc1-aeaa-d344b6fb8bfe
2025-08-14T16:18:58.529+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 🖨️ Uploaded 0 print files
2025-08-14T16:18:58.529+02:00 ERROR 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : ⚠️ Failed to scan for .trigger file in jobDir: /mnt/nfs/ete-exstream/ete-SA/output/MFC/010dd1e9-4c43-4fc1-aeaa-d344b6fb8bfe

java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/MFC/010dd1e9-4c43-4fc1-aeaa-d344b6fb8bfe
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
	at java.base/sun.nio.fs.UnixFileSystemProvider.newDirectoryStream(UnixFileSystemProvider.java:440) ~[na:na]
	at java.base/java.nio.file.Files.newDirectoryStream(Files.java:482) ~[na:na]
	at java.base/java.nio.file.Files.list(Files.java:3792) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.findAndUploadMobstatTriggerFile(KafkaListenerService.java:394) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:339) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:203) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-08-14T16:18:58.531+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📱 Found Mobstat URL: null
2025-08-14T16:18:59.629+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-08-14T16:18:59.630+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-08-14T16:18:59.630+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-08-14T16:19:00.031+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : 📄 Extracted summary counts from _STDDELIVERYFILE.xml: customersProcessed=10635, pagesProcessed=36376
2025-08-14T16:19:00.621+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON written at: /tmp/summaryFiles8131523680263993323/summary_81d7dfb9-cb41-4a47-8438-8e686b0aec52.json
2025-08-14T16:19:01.332+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.BlobStorageService       : 📤 Uploaded file to 'https://nsnetextr01.blob.core.windows.net/nsneteextrm/MFC%2F81d7dfb9-cb41-4a47-8438-8e686b0aec52%2F19ef9d68-b114-4803-b09b-95a6c5fa4644%2Fsummary_81d7dfb9-cb41-4a47-8438-8e686b0aec52.json'
2025-08-14T16:19:01.332+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📁 Summary JSON uploaded to: https://nsnetextr01.blob.core.windows.net/nsneteextrm/MFC/81d7dfb9-cb41-4a47-8438-8e686b0aec52/19ef9d68-b114-4803-b09b-95a6c5fa4644/summary_81d7dfb9-cb41-4a47-8438-8e686b0aec52.json
2025-08-14T16:19:02.026+02:00  INFO 1 --- [pool-1-thread-3] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] 📄 Final Summary Payload:
{
  "batchID" : "81d7dfb9-cb41-4a47-8438-8e686b0aec52",
  "fileName" : "NDDSST_250723.DAT, NDDSSTBR_250430_error.DAT",
  "header" : {
    "tenantCode" : "ZANBL",
    "channelID" : "Test123",
    "audienceID" : null,
    "timestamp" : "1754992688",
    "sourceSystem" : "MFC",
    "product" : "MFC",
    "jobName" : "MFC"
  },
  "metadata" : {
    "totalCustomersProcessed" : 10635,
    "processingStatus" : "FAILED",
    "eventOutcomeCode" : "0",
    "eventOutcomeDescription" : "failed"
  },
  "payload" : {
    "uniqueConsumerRef" : null,
    "uniqueECPBatchRef" : null,
    "runPriority" : null,
    "eventID" : null,
    "eventType" : null,
    "restartKey" : null,
    "fileCount" : 0
  },
  "processedFileList" : [ {
    "customerId" : "110460266500",
    "accountNumber" : "01039930014",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  }, {
    "customerId" : "110053208005",
    "accountNumber" : "03554450013",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  }, {
    "customerId" : "191053611201",
    "accountNumber" : "06617912849",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  }, {
    "customerId" : "193030169204",
    "accountNumber" : "06717030007",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  }, {
    "customerId" : "191205670209",
    "accountNumber" : "06827053778",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  }, {
    "customerId" : "191260344504",
    "accountNumber" : "06881500731",
    "emailStatus" : "",
    "printStatus" : "",
    "mobstatStatus" : "",
    "overallStatus" : "FAILED"
  },
