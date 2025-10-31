FileManager – Kafka Message and Audit Enhancement Documentation
1. Overview

This document outlines the new Kafka message schema and the audit enhancements implemented in the FileManager service.
The enhancements ensure end-to-end traceability across all FileManager processing stages — from InboundListener message ingestion to Fmcompose and Fmcomplete audit publishing.

2. Updated Kafka Message Structure
2.1 Message Description

The FileManager now consumes messages with the following enhanced structure.
The new format includes additional metadata fields such as dataStreamName, sourceSystem, product, and detailed batchFiles information.

2.2 Sample Kafka Message (InboundListener)
{
  "dataStreamName": "InboundListener",
  "dataStreamType": "logs",
  "serviceName": "InboundListener",
  "sourceSystem": "ROA-ABF-LEND",
  "serviceEnv": "DEV",
  "batchId": "347bb96e-56a7-4d56-9fc7-b5573224a31a",
  "tenantCode": "ZANBL",
  "channelID": "chnl007",
  "audienceID": null,
  "product": "ROA-ABF-LEND",
  "jobName": "NAM_ROA_ABF_LENDING_STMT",
  "consumerRef": "30OCT1033-b119-5506-b09b-95a6c5fa4644",
  "timestamp": 1761743258.680999100,
  "startTime": 1761743255878,
  "endTime": 1761743258680,
  "runPriority": null,
  "eventType": null,
  "batchFiles": [
    {
      "objectId": "idd_504CBE99-0000-C016-B507-32F548FAAB17",
      "repositoryId": "BATCH",
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
      "fileName": "NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
      "fileType": "DATA",
      "validationStatus": "Valid",
      "customerCount": 10
    }
  ]
}

2.3 Key Additions
Field	Description
dataStreamName	Identifies the service or component producing the event.
sourceSystem	Originating system of the batch (e.g., ROA-ABF-LEND).
product	Product or domain associated with the source system.
batchFiles	Array containing blob URL, filename, type, and customer count.
consumerRef	Unique reference ID for consumer transaction traceability.
startTime / endTime	Time window for batch execution.
3. Audit Message – Fmcompose
3.1 Description

During composition (after initial processing and validation),
FileManager publishes an Audit message to the Audit topic under the data stream name Fmcompose.

3.2 Sample Audit Message
{
  "title": "ECPBatchAudit",
  "type": "object",
  "datastreamName": "Fmcompose",
  "datastreamType": "logs",
  "batchId": "347bb96e-56a7-4d56-9fc7-b5573224a31a",
  "serviceName": "Fmcompose",
  "systemEnv": "DEV",
  "sourceSystem": "ROA-ABF-LEND",
  "tenantCode": "ZANBL",
  "channelId": "chnl007",
  "audienceId": null,
  "product": "ROA-ABF-LEND",
  "jobName": "NAM_ROA_ABF_LENDING_STMT",
  "consumerRef": "30OCT1033-b119-5506-b09b-95a6c5fa4644",
  "timestamp": "2025-10-30T08:34:16.353778370Z",
  "eventType": null,
  "startTime": "2025-10-30T08:34:16.268227549Z",
  "endTime": "2025-10-30T08:34:16.268227549Z",
  "customerCount": 10,
  "batchFiles": [
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
      "fileName": "NAM_ROA_ABF_LENDING_STMT_20250926_Test.TXT",
      "fileType": "DATA"
    }
  ],
  "success": true,
  "errorCode": null,
  "errorMessage": null,
  "retryFlag": false,
  "retryCount": 0
}

3.3 Purpose

Confirms that batch composition is complete.

Provides traceability for each input blob and batch details.

Acts as a trigger for downstream validation or next stage (Fmcomplete).

4. Composition Completion – Kafka Output

After FileManager completes message processing and summary file generation,
it produces a final Kafka response representing the summary payload.

4.1 Example Output Message
{
  "batchID": "968731bd-00f8-4139-af30-4ebea2305cc8",
  "fileName": "NDDSSTBR_251027.DAT, NDDSST_251028.DAT",
  "header": {
    "tenantCode": "ZANBL",
    "channelID": "chnl007",
    "audienceID": null,
    "timestamp": "1.7618205279680977E9",
    "sourceSystem": "MFC",
    "product": "MFC",
    "jobName": "MFC"
  },
  "metadata": {
    "totalCustomersProcessed": 141,
    "processingStatus": "PARTIAL",
    "eventOutcomeCode": "0",
    "eventOutcomeDescription": "partial",
    "customerCount": 141
  },
  "payload": {
    "uniqueConsumerRef": null,
    "uniqueECPBatchRef": null,
    "runPriority": null,
    "eventID": null,
    "eventType": null,
    "restartKey": null,
    "fileCount": 220
  },
  "summaryFileURL": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/MFC/968731bd-00f8-4139-af30-4ebea2305cc8/19ef9d68-b119-5506-b09b-95a6c5fa4644/summary_968731bd-00f8-4139-af30-4ebea2305cc8.json",
  "timestamp": "1.7618205279680977E9"
}

5. Audit Message – Fmcomplete
5.1 Description

Once summary generation and all downstream actions are complete,
FileManager publishes a final Audit message under the data stream name Fmcomplete.

5.2 Sample Audit Message
{
  "title": "ECPBatchAudit",
  "type": "object",
  "datastreamName": "Fmcomplete",
  "datastreamType": "logs",
  "batchId": "968731bd-00f8-4139-af30-4ebea2305cc8",
  "serviceName": "Fmcomplete",
  "systemEnv": "DEV",
  "sourceSystem": "MFC",
  "tenantCode": "ZANBL",
  "channelId": "chnl007",
  "audienceId": null,
  "product": "MFC",
  "jobName": "5a895fe8-ab67-4c85-b1ef-6f82f7ea86b6",
  "consumerRef": "19ef9d68-b119-5506-b09b-95a6c5fa4644",
  "timestamp": "2025-10-30T10:37:59.264480312Z",
  "eventType": null,
  "startTime": "2025-10-30T10:37:05.951516298Z",
  "endTime": "2025-10-30T10:37:59.264469112Z",
  "customerCount": 141,
  "batchFiles": [
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/MFC/968731bd-00f8-4139-af30-4ebea2305cc8/19ef9d68-b119-5506-b09b-95a6c5fa4644/summary_968731bd-00f8-4139-af30-4ebea2305cc8.json",
      "fileName": "summary_968731bd-00f8-4139-af30-4ebea2305cc8.json",
      "fileType": "json"
    }
  ],
  "success": true,
  "errorCode": null,
  "errorMessage": null,
  "retryFlag": false,
  "retryCount": 0
}

5.3 Purpose

Signifies completion of batch processing and summary upload.

Provides complete audit information, including summary file location and customer counts.

Enables full traceability and monitoring of the FileManager workflow.

6. Summary of Flow
Stage	Data Stream	Description	Output
InboundListener	InboundListener	Receives new Kafka messages containing batch metadata and file details.	Raw Kafka message with batch information.
Fmcompose	Fmcompose	Triggered after file validation and preparation.	Audit message confirming composition completion.
FileManager Processing	—	Generates customer output files and summary.json.	Summary payload returned to Kafka.
Fmcomplete	Fmcomplete	Final audit event after summary upload.	Audit confirmation with summary file URL.
7. Benefits of Enhancement

✅ Unified audit tracking across FileManager stages.

✅ Improved traceability with consistent identifiers (batchId, consumerRef).

✅ Standardized Kafka message schema for upstream/downstream systems.

✅ Better monitoring and debugging via Audit topic events (Fmcompose, Fmcomplete).
