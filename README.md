2025-07-18T11:49:09.593+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-07-18T11:49:10.485+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-07-18T11:49:10.485+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-07-18T11:49:10.485+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1752832150485
2025-07-18T11:49:10.488+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@4d608c2]
2025-07-18T11:49:11.087+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition str-ecp-batch-composition-complete-0 to 32 since the associated topicId changed from null to kFgHvXbjT6KB5Z2coZAcJw
2025-07-18T11:49:11.088+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-07-18T11:49:11.088+02:00  INFO 1 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 196312 with epoch 0
2025-07-18T11:49:11.284+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 OT request sent for batch 54bf11dc-a912-490b-b02f-7a0aa228ee06
2025-07-18T11:49:11.287+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18835, leaderEpoch=null, metadata=''}}
2025-07-18T11:49:11.387+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ⏳ Waiting for XML for jobId=5965a65c-b8cd-4228-b6d3-ebeb5040c2ed, id=9792fbea-cc74-4567-8962-4f3876540c1a
2025-07-18T11:49:16.388+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-18T11:49:17.857+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found stable XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/5965a65c-b8cd-4228-b6d3-ebeb5040c2ed/9792fbea-cc74-4567-8962-4f3876540c1a/docgen/3508bb30-b6fc-4f82-85ab-273a2cc68b02/output/_STDDELIVERYFILE.xml
2025-07-18T11:49:17.858+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/5965a65c-b8cd-4228-b6d3-ebeb5040c2ed/9792fbea-cc74-4567-8962-4f3876540c1a/docgen/3508bb30-b6fc-4f82-85ab-273a2cc68b02/output/_STDDELIVERYFILE.xml
2025-07-18T11:49:17.883+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🧾 Parsed error report with 0 entries
2025-07-18T11:49:18.892+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📊 Total customerSummaries parsed: 39
2025-07-18T11:49:20.395+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F1001382854_EMLCA002.pdf'
2025-07-18T11:49:20.788+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F1001451856_error.pdf'
2025-07-18T11:49:21.389+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-18T11:49:21.608+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F1001499298_EMLCA002.pdf'
2025-07-18T11:49:22.085+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F1125950300101_LHDL03E.pdf'
2025-07-18T11:49:22.683+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F3768000010570766_EML002.pdf'
2025-07-18T11:49:23.284+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F5898460773614342_EML002.pdf'
2025-07-18T11:49:23.696+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F8637238800101_LHDL03E.pdf'
2025-07-18T11:49:24.090+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Femail%2F8835308600101_LHDL03E.pdf'
2025-07-18T11:49:24.684+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F1001179722_EMLCA002.pdf'
2025-07-18T11:49:25.183+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F3768000010613657_CCMOB805.pdf'
2025-07-18T11:49:25.585+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774023071_CCMOB805.pdf'
2025-07-18T11:49:25.983+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774023931_error.pdf'
2025-07-18T11:49:26.390+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-18T11:49:26.484+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774025233_CCMOB805.pdf'
2025-07-18T11:49:27.201+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774025472_CCMOB805.pdf'
2025-07-18T11:49:27.684+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774027833_CCMOB805.pdf'
2025-07-18T11:49:28.187+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774028104_CCMOB805.pdf'
2025-07-18T11:49:28.584+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F5898460774031439_CCMOB805.pdf'
2025-07-18T11:49:28.897+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded BINARY file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/out%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2Farchive%2F8186138500101_LHDL03S.pdf'
2025-07-18T11:49:29.008+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📦 Processed 20 customer records
2025-07-18T11:49:29.008+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🖨️ Uploaded 0 print files
2025-07-18T11:49:29.083+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ℹ️ No .trigger file found in jobDir: /mnt/nfs/dev-exstream/dev-SA/output/DEBTMAN/5965a65c-b8cd-4228-b6d3-ebeb5040c2ed
2025-07-18T11:49:29.486+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON written at: /tmp/summaryFiles3410211486894896760/summary_unknown.json
2025-07-18T11:49:29.702+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Fsummary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json'
2025-07-18T11:49:29.702+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📁 Summary JSON uploaded to: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/54bf11dc-a912-490b-b02f-7a0aa228ee06/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/summary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json
2025-07-18T11:49:29.790+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📄 Final Summary Payload:
{
  "fileName" : "DEBTMAN_20250715_022024_error.TXT",
  "header" : {
    "tenantCode" : "ZANBL",
    "channelID" : null,
    "audienceID" : null,
    "timestamp" : "2025-07-18T09:49:29.287167709Z",
    "sourceSystem" : "DEBTMAN",
    "product" : "DEBTMAN",
    "jobName" : "DEBTMAN"
  },
  "metadata" : {
    "totalFilesProcessed" : 20,
    "processingStatus" : "Completed",
    "eventOutcomeCode" : "0",
    "eventOutcomeDescription" : "Success"
  },
  "payload" : {
    "uniqueConsumerRef" : null,
    "uniqueECPBatchRef" : null,
    "runPriority" : null,
    "eventID" : null,
    "eventType" : null,
    "restartKey" : null,
    "fileCount" : 20
  },
  "processedFileList" : [ {
    "customerId" : "191749661002",
    "accountNumber" : "1001179722",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600002895121",
    "accountNumber" : "1001382854",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006332098",
    "accountNumber" : "error",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708711",
    "accountNumber" : "5898460774025472",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708291",
    "accountNumber" : "5898460774031439",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "110051703106",
    "accountNumber" : "5898460773614342",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "110000715808",
    "accountNumber" : "1125950300101",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "191752943609",
    "accountNumber" : "1001499298",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "110000206903",
    "accountNumber" : "8835308600101",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708608",
    "accountNumber" : "5898460774023931",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600005484029",
    "accountNumber" : "3768000010570766",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "110000365408",
    "accountNumber" : "8186138500101",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006709118",
    "accountNumber" : "5898460774023071",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708154",
    "accountNumber" : "5898460774028104",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "110000206903",
    "accountNumber" : "8637238800101",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "191590092008",
    "accountNumber" : "1001451856",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006709129",
    "accountNumber" : "5898460774027833",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708797",
    "accountNumber" : "3768000010613657",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "600006708699",
    "accountNumber" : "5898460774025233",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  }, {
    "customerId" : "211432335427_ARC",
    "accountNumber" : "5898460761746866",
    "pdfArchiveFileUrl" : null,
    "pdfArchiveFileUrlStatus" : null,
    "pdfEmailFileUrl" : null,
    "pdfEmailFileUrlStatus" : null,
    "printFileUrl" : null,
    "printFileUrlStatus" : null,
    "pdfMobstatFileUrl" : null,
    "pdfMobstatFileUrlStatus" : null,
    "statusCode" : "OK",
    "statusDescription" : "Success"
  } ],
  "timestamp" : "2025-07-18T09:49:29.287167709Z",
  "customerSummaries" : [ ]
}
