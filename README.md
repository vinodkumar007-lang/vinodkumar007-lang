2025-07-16T16:55:47.386+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📁 Created input directory: /mnt/nfs/dev-exstream/dev-SA/input/DEBTMAN/54bf11dc-a912-490b-b02f-7a0aa228ee06
2025-07-16T16:55:47.901+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ⬇️ Downloaded file https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN_20250715_022024_error.TXT to local path /mnt/nfs/dev-exstream/dev-SA/input/DEBTMAN/54bf11dc-a912-490b-b02f-7a0aa228ee06/DEBTMAN_20250715_022024_error.TXT
2025-07-16T16:55:47.901+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🚀 Calling Orchestration API: http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/batch/dev-SA/ECPDebtmanService
2025-07-16T16:55:48.485+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 OT request sent for batch 54bf11dc-a912-490b-b02f-7a0aa228ee06
2025-07-16T16:55:48.485+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18791, leaderEpoch=null, metadata=''}}
2025-07-16T16:55:48.490+02:00  INFO 1 --- [pool-1-thread-4] c.n.k.f.service.KafkaListenerService     : ⏳ Waiting for XML for jobId=b22f897e-297f-43a7-8374-f276f849d847, id=0899207b-8b4d-4055-bdf1-ebde86005f71
2025-07-16T16:55:51.545+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found stable XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/2c2bb199-401c-49cf-a042-0a7c8b46e02a/03fca261-155c-4b89-9554-ba6d5ee05cb6/docgen/01c510f9-d1f3-4721-abfc-1159e246563c/output/_STDDELIVERYFILE.xml
2025-07-16T16:55:51.546+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/2c2bb199-401c-49cf-a042-0a7c8b46e02a/03fca261-155c-4b89-9554-ba6d5ee05cb6/docgen/01c510f9-d1f3-4721-abfc-1159e246563c/output/_STDDELIVERYFILE.xml
2025-07-16T16:55:52.510+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🧾 Parsed error report with 0 entries
2025-07-16T16:55:52.511+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📦 Processed 0 customer records
2025-07-16T16:55:52.587+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🖨️ Uploaded 0 print files
2025-07-16T16:55:52.596+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ℹ️ No .trigger file found in jobDir: /mnt/nfs/dev-exstream/dev-SA/output/DEBTMAN/2c2bb199-401c-49cf-a042-0a7c8b46e02a
2025-07-16T16:55:52.609+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON written at: /tmp/summaryFiles3416708873452464506/summary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json
2025-07-16T16:55:53.489+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-16T16:55:53.589+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Fsummary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json'
2025-07-16T16:55:53.589+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📁 Summary JSON uploaded to: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/54bf11dc-a912-490b-b02f-7a0aa228ee06/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/summary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json
2025-07-16T16:55:53.692+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📄 Final Summary Payload:
{
  "batchID" : "54bf11dc-a912-490b-b02f-7a0aa228ee06",
  "fileName" : "DEBTMAN_20250715_022024_error.TXT",
  "header" : {
    "tenantCode" : "ZANBL",
    "channelID" : null,
    "audienceID" : null,
    "timestamp" : "2025-07-16T14:55:52.596944759Z",
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
    "uniqueConsumerRef" : "19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs",
    "uniqueECPBatchRef" : null,
    "runPriority" : null,
    "eventID" : null,
    "eventType" : null,
    "restartKey" : null,
    "fileCount" : 0
  },
  "processedFiles" : [ ],
  "printFiles" : [ ],
  "timestamp" : "2025-07-16T14:55:52.596944759Z"
}
