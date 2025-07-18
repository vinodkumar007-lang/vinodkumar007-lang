2025-07-18T05:31:47.285+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-07-18T05:31:47.286+02:00  INFO 1 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 196307 with epoch 0
2025-07-18T05:31:47.490+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 OT request sent for batch 54bf11dc-a912-490b-b02f-7a0aa228ee06
2025-07-18T05:31:47.492+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18826, leaderEpoch=null, metadata=''}}
2025-07-18T05:31:47.685+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ⏳ Waiting for XML for jobId=c10b50de-23ad-4a57-8cc4-a1949889f5b9, id=dd70b448-382e-45be-9923-66e388b2e49e
2025-07-18T05:31:52.683+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-18T05:31:54.155+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found stable XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/c10b50de-23ad-4a57-8cc4-a1949889f5b9/dd70b448-382e-45be-9923-66e388b2e49e/docgen/1f496bd0-2110-4f12-830e-e5dd31961fc3/output/_STDDELIVERYFILE.xml
2025-07-18T05:31:54.155+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/c10b50de-23ad-4a57-8cc4-a1949889f5b9/dd70b448-382e-45be-9923-66e388b2e49e/docgen/1f496bd0-2110-4f12-830e-e5dd31961fc3/output/_STDDELIVERYFILE.xml
2025-07-18T05:31:54.175+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🧾 Parsed error report with 0 entries
2025-07-18T05:31:55.086+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📊 Total customerSummaries parsed: 39
2025-07-18T05:31:55.139+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📦 Processed 0 customer records
2025-07-18T05:31:55.140+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🖨️ Uploaded 0 print files
2025-07-18T05:31:55.147+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ℹ️ No .trigger file found in jobDir: /mnt/nfs/dev-exstream/dev-SA/output/DEBTMAN/c10b50de-23ad-4a57-8cc4-a1949889f5b9
2025-07-18T05:31:55.298+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON written at: /tmp/summaryFiles12525728915688943916/summary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json
2025-07-18T05:31:56.385+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.BlobStorageService       : 📤 Uploaded TEXT file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F54bf11dc-a912-490b-b02f-7a0aa228ee06%2F19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs%2Fsummary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json'
2025-07-18T05:31:56.386+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📁 Summary JSON uploaded to: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/54bf11dc-a912-490b-b02f-7a0aa228ee06/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/summary_54bf11dc-a912-490b-b02f-7a0aa228ee06.json
2025-07-18T05:31:56.585+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📄 Final Summary Payload:
{
  "batchID" : "54bf11dc-a912-490b-b02f-7a0aa228ee06",
  "fileName" : "DEBTMAN_20250715_022024_error.TXT",
  "header" : {
    "tenantCode" : "ZANBL",
    "channelID" : null,
    "audienceID" : null,
    "timestamp" : "2025-07-18T03:31:55.287476867Z",
    "sourceSystem" : "DEBTMAN",
    "product" : "DEBTMAN",
    "jobName" : "DEBTMAN"
  },
  "metadata" : {
    "totalFilesProcessed" : 24,
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
    "fileCount" : 38
  },
  "printFiles" : [ ],
  "timestamp" : "2025-07-18T03:31:55.287476867Z",
  "customerSummaries" : [ ]
}
