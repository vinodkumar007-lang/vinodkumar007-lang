📄 Document Details

Field

Value

Client

Nedbank

Project

ECM File Manager

Prepared By

File-Manager Dev Team

Reviewed By

Solution Architect Team

Document Date

June 25, 2025

Version

1.0

🔍 Purpose
The File-Manager service is responsible for orchestrating the processing of customer communication files. It integrates with Kafka, OpenText, Azure Key Vault, and Azure Blob Storage to generate and store output files and metadata.

⚙️ High-Level Architecture
• Kafka: Message broker for input and output messages• OpenText: External system for document processing• Azure Key Vault: Secure storage for secrets• Azure Blob Storage: Storage for generated summary files• File-Manager: Core Spring Boot service

📝 End-to-End Workflow

✅ Step 0: Validate fileType and Apply Batch Processing Rules• Each Kafka message is consumed using Spring’s @KafkaListener configured on the input topic.• The message includes:

batchId, fileName, sourceSystem, blobUrl, fileType• The following rules apply to determine if the batch should be processed:

FileType Scenario

Action

1 × DATA

✅ Process batch

1 × DATA + 1+ REF

✅ Process only DATA

2 or more × DATA

❌ Skip batch

Only REF files

❌ Skip batch

No files / Empty payload

❌ Skip batch

• If a batch is skipped, no downstream processing (OpenText/API/Blob) is performed.• Only the valid case of a single DATA file (with or without REF) is eligible for full processing.

✅ Step 0.1: Validate fileType and Apply Batch Processing Rules• Each Kafka message is validated based on its fileType.• The following rules apply:

FileType Scenario

Action

1 × DATA

✅ Process batch

1 × DATA + 1+ REF

✅ Process only DATA

2 or more × DATA

❌ Skip batch

Only REF files

❌ Skip batch

No files / Empty payload

❌ Skip batch

• If a batch is skipped, no downstream processing (OpenText/API/Blob) is performed.• Only the valid case of a single DATA file (with or without REF) is eligible for full processing.

✅ Step 1: Read Kafka Message from Input Topic• File-Manager consumes a message from Kafka input topic (str-ecp-batch-composition).• The message includes:

batchId, fileName, sourceSystem, blobUrl• Required authentication and authorization are already configured (SSL-enabled Kafka consumer).

✅ Step 2: Send Kafka Message to OpenText• File-Manager constructs a Kafka message containing:

Metadata about the file.

Instructions for OpenText to process the file.• Sends the message to API service call:https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService

✅ Step 3: OpenText Processes and Sends Response• OpenText system:

Processes the file.

Generates output (HTML, PDF, MOBSTAT, PRINT files).

Sends a response message back to File-Manager via API:https://dev-exstream.nednet.co.za/api/file/processed

✅ Step 4: Read OpenText Response & Prepare Summary File• File-Manager consumes the response message.• Prepares a structured summary.json:

batchId, fileName, Timestamps, Processed files, Blob URLs, Status

✅ Step 5: Connect to Azure Key Vault (Authentication)• File-Manager connects to Azure Key Vault.• Uses:

clientId (App registration on AD)

tenantId• Authentication is done via Azure AD.

✅ Step 6: Retrieve Secrets from Key Vault• Secrets retrieved:

accountKey, accountName, containerName• Secrets are required to access Azure Blob Storage.

✅ Step 7: Connect to Azure Blob Storage• Using retrieved credentials, File-Manager establishes connection with Azure Blob Storage.

✅ Step 8: Store Summary File in Blob Storage• Upload summary.json into Azure Blob Storage:

Folder structure:/containerName/{sourceSystem}/{batchId}/summary.json

✅ Step 9: Send Kafka Message to Output Topic• File-Manager sends a Kafka message to the output topic (str-ecp-batch-composition-complete):

Contains batchId, fileName, status, and blob URL.

✅ Step 10: Return Final API Response• File-Manager sends final REST API response:

Processing status (SUCCESS / FAILURE)

Summary file Blob URL

Kafka output message details

📚 Technologies Used

Component

Technology

Messaging

Apache Kafka (SSL enabled)

Document Processing

OpenText

Secrets Management

Azure Key Vault

Storage

Azure Blob Storage

Application Framework

Spring Boot

📊 Flow DiagramKafka Input Topic → File-Manager (@KafkaListener) → Check fileType → Kafka Message to OpenText → OpenText Processes → API service call →File-Manager → Prepare Summary.json → Connect to KeyVault → Get Secrets → Connect to Blob Storage →Upload Summary File → Send Kafka Output Message → Return API Response

🧾 Sample Final API Response
{
"message": "Batch processed successfully",
"status": "success",
"summaryPayload": {
"batchID": "2c93525b-42d1-410a-9e26-aa957f19861d",
"fileName": "DEBTMAN.csv",
"header": {
"tenantCode": "ZANBL",
"channelID": null,
"audienceID": null,
"timestamp": "1970-01-21T05:39:11.245Z",
"sourceSystem": "DEBTMAN",
"product": "DEBTMAN",
"jobName": "DEBTMAN"
},
"metadata": {
"totalFilesProcessed": 11,
"processingStatus": "Completed",
"eventOutcomeCode": "0",
"eventOutcomeDescription": "Success"
},
"payload": {
"uniqueConsumerRef": "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
"uniqueECPBatchRef": null,
"runPriority": null,
"eventID": null,
"eventType": null,
"restartKey": null,
"fileCount": 11
},
"summaryFileURL": "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/summary_2c93525b-42d1-410a-9e26-aa957f19861d.json",
"timestamp": "2025-06-17T08:37:07.599978170Z"
}
}

🧾 Sample Summary file
{
"batchID" : "2c93525b-42d1-410a-9e26-aa957f19861d",
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
"totalFilesProcessed" : 11,
"processingStatus" : "Completed",
"eventOutcomeCode" : "0",
"eventOutcomeDescription" : "Success"
},
"payload" : {
"uniqueConsumerRef" : "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
"uniqueECPBatchRef" : null,
"runPriority" : null,
"eventID" : null,
"eventType" : null,
"restartKey" : null,
"fileCount" : 11
},
"processedFiles" : [ {
"customerId" : "110543680509",
"accountNumber" : "3768000010607501",
"pdfArchiveFileUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110543680509_12485337728657340876.pdf",
"pdfEmailFileUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110543680509_12485337728657340876.pdf",
"htmlEmailFileUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110543680509_15674937613143496857.html",
"txtEmailFileUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110543680509_4155712775909391580.txt",
"pdfMobstatFileUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F2c93525b-42d1-410a-9e26-aa957f19861d%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110543680509_3101796713995731386.mobstat",
"statusCode" : "OK",
"statusDescription" : "Success"
} ],
"printFiles" : [ {
"printFileURL" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/1970-01-21/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN/print/2c93525b-42d1-410a-9e26-aa957f19861d_printfile.pdf"
} ],
"mobstatTriggerFile" : "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/mobstat/DropData.trigger"
}

