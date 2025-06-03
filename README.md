2025-06-03T15:14:06.202+02:00  WARN 2560 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Error processing file 'DEBTMAN.csv': The filename, directory name, or volume label syntax is incorrect
2025-06-03T15:14:06.277+02:00  INFO 2560 --- [nio-8080-exec-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON successfully written.
2025-06-03T15:14:06.277+02:00  INFO 2560 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : 📄 Summary JSON content before upload:
{
  "batchID" : "1c93525b-42d1-410a-9e26-aa957f19861c",
  "fileName" : "DEBTMAN.csv",
  "header" : {
    "tenantCode" : "ZANBL",
    "channelID" : null,
    "audienceID" : null,
    "timestamp" : "1970-01-21T05:39:11.245Z",
    "sourceSystem" : "DEBTMAN",
    "product" : "DEBTMAN",
    "jobName" : "DEBTMAN"
  },
  "metadata" : {
    "totalFilesProcessed" : 0,
    "processingStatus" : null,
    "eventOutcomeCode" : null,
    "eventOutcomeDescription" : null,
    "processedFileList" : [ ],
    "printFile" : [ {
      "printFileURL" : "https://https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001.blob.core.windows.net/DEBTMAN/+150368834/1c93525b-42d1-410a-9e26-aa957f19861c/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN/print/1c93525b-42d1-410a-9e26-aa957f19861c_print.pdf"
    } ]
  },
  "payload" : {
    "uniqueConsumerRef" : "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
    "uniqueECPBatchRef" : null,
    "runPriority" : null,
    "eventID" : null,
    "eventType" : null,
    "restartKey" : null,
    "fileCount" : 0
  },
  "processedFiles" : null,
  "printFiles" : null,
  "summaryFileURL" : null,
  "fileLocation" : null,
  "timestamp" : null
}
2025-06-03T15:14:06.291+02:00  INFO 2560 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : ✅ Summary JSON written to file: summary.json
2025-06-03T15:14:06.514+02:00  INFO 2560 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F%2B150368834%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fsummary.json'
2025-06-03T15:14:06.532+02:00  INFO 2560 --- [nio-8080-exec-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 


{
    "message": null,
    "status": null,
    "data": null,
    "summaryPayload": null
}
