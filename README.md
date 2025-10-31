new kafka message format:
=========
{
  "dataStreamName" : "InboundListener",
  "dataStreamType" : "logs",
  "serviceName" : "InboundListener",
  "sourceSystem" : "ROA-ABF-LEND",
  "serviceEnv" : "DEV",
  "batchId" : "347bb96e-56a7-4d56-9fc7-b5573224a31a",
  "tenantCode" : "ZANBL",
  "channelID" : "chnl007",
  "audienceID" : null,
  "product" : "ROA-ABF-LEND",
  "jobName" : "NAM_ROA_ABF_LENDING_STMT",
  "consumerRef" : "30OCT1033-b119-5506-b09b-95a6c5fa4644",
  "timestamp" : 1761743258.680999100,
  "startTime" : 1761743255878,
  "endTime" : 1761743258680,
  "runPriority" : null,
  "eventType" : null,
  "batchFiles" : [ {
    "objectId" : "idd_504CBE99-0000-C016-B507-32F548FAAB17",
    "repositoryId" : "BATCH",
    "blobUrl" : "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
    "fileName" : "NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
    "fileType" : "DATA",
    "validationStatus" : "Valid",
    "customerCount" : 10
  } ]
}


Fmcompose:
=================
📣 Audit message sent successfully for batchId 347bb96e-56a7-4d56-9fc7-b5573224a31a: {"title":"ECPBatchAudit","type":"object","datastreamName":"Fmcompose","datastreamType":"logs","batchId":"347bb96e-56a7-4d56-9fc7-b5573224a31a","serviceName":"Fmcompose","systemEnv":"DEV","sourceSystem":"ROA-ABF-LEND","tenantCode":"ZANBL","channelId":"chnl007","audienceId":null,"product":"ROA-ABF-LEND","jobName":"NAM_ROA_ABF_LENDING_STMT","consumerRef":"30OCT1033-b119-5506-b09b-95a6c5fa4644","timestamp":"2025-10-30T08:34:16.353778370Z","eventType":null,"startTime":"2025-10-30T08:34:16.268227549Z","endTime":"2025-10-30T08:34:16.268227549Z","customerCount":10,"batchFiles":[{"blobUrl":"https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT","fileName":"NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT","fileType":"DATA"}],"success":true,"errorCode":null,"errorMessage":null,"retryFlag":false,"retryCount":0}

composition complete:
===========
✅ Kafka output sent with response: {"batchID":"968731bd-00f8-4139-af30-4ebea2305cc8","fileName":"NDDSSTBR_251027.DAT, NDDSST_251028.DAT","header":{"tenantCode":"ZANBL","channelID":"chnl007","audienceID":null,"timestamp":"1.7618205279680977E9","sourceSystem":"MFC","product":"MFC","jobName":"MFC"},"metadata":{"totalCustomersProcessed":141,"processingStatus":"PARTIAL","eventOutcomeCode":"0","eventOutcomeDescription":"partial","customerCount":141},"payload":{"uniqueConsumerRef":null,"uniqueECPBatchRef":null,"runPriority":null,"eventID":null,"eventType":null,"restartKey":null,"fileCount":220},"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsndevextrm01/MFC/968731bd-00f8-4139-af30-4ebea2305cc8/19ef9d68-b119-5506-b09b-95a6c5fa4644/summary_968731bd-00f8-4139-af30-4ebea2305cc8.json","timestamp":"1.7618205279680977E9"}

**Fmcomplete:**
============================
2025-10-30T12:37:59.350+02:00  INFO 1 --- [ad | producer-1] c.n.k.f.service.KafkaListenerService     : 📣 Audit message sent successfully for batchId 968731bd-00f8-4139-af30-4ebea2305cc8: {"title":"ECPBatchAudit","type":"object","datastreamName":"Fmcomplete","datastreamType":"logs","batchId":"968731bd-00f8-4139-af30-4ebea2305cc8","serviceName":"Fmcomplete","systemEnv":"DEV","sourceSystem":"MFC","tenantCode":"ZANBL","channelId":"chnl007","audienceId":null,"product":"MFC","jobName":"5a895fe8-ab67-4c85-b1ef-6f82f7ea86b6","consumerRef":"19ef9d68-b119-5506-b09b-95a6c5fa4644","timestamp":"2025-10-30T10:37:59.264480312Z","eventType":null,"startTime":"2025-10-30T10:37:05.951516298Z","endTime":"2025-10-30T10:37:59.264469112Z","customerCount":141,"batchFiles":[{"blobUrl":"https://nsndvextr01.blob.core.windows.net/nsndevextrm01/MFC/968731bd-00f8-4139-af30-4ebea2305cc8/19ef9d68-b119-5506-b09b-95a6c5fa4644/summary_968731bd-00f8-4139-af30-4ebea2305cc8.json","fileName":"summary_968731bd-00f8-4139-af30-4ebea2305cc8.json","fileType":"json"}],"success":true,"errorCode":null,"errorMessage":null,"retryFlag":false,"retryCount":0}
