🔍 Purpose
The File-Manager service is responsible for orchestrating the processing of customer communication files. It integrates with Kafka, OpenText, Azure Key Vault, and Azure Blob Storage to generate and store output files and metadata.
________________________________________
⚙️ High-Level Architecture
•	Kafka: Message broker for input and output messages
•	OpenText: External system for document processing
•	Azure Key Vault: Secure storage for secrets
•	Azure Blob Storage: Storage for generated summary files
•	File-Manager: Core Spring Boot service
________________________________________
📝 End-to-End Workflow
________________________________________
✅ Step 1: Read Kafka Message from Input Topic
•	File-Manager consumes a message from Kafka input topic (str-ecp-batch-composition).
•	The message includes:
o	batchId
o	fileName
o	sourceSystem
o	blobUrl
•	Required authentication and authorization are already configured (SSL-enabled Kafka consumer).
________________________________________
✅ Step 2: Send Kafka Message to OpenText
•	File-Manager constructs a Kafka message containing:
o	Metadata about the file.
o	Instructions for OpenText to process the file.
•	Sends the message to API service call (https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService)
________________________________________
✅ Step 3: OpenText Processes and Sends Response
•	OpenText system:
o	Processes the file.
o	Generates output (HTML, PDF, MOBSTAT, PRINT files).
o	Sends a response message back to File-Manager via API service call (https://dev-exstream.nednet.co.za/api/file/processed).
________________________________________
✅ Step 4: Read OpenText Response & Prepare Summary File
•	File-Manager consumes the response message.
•	Prepares a structured summary.json:
o	batchId
o	fileName
o	Timestamps
o	Processed files details
o	Blob URLs of generated files
o	Status (SUCCESS / FAILURE)
________________________________________
✅ Step 5: Connect to Azure Key Vault (Authentication)
•	File-Manager connects to Azure Key Vault.
•	Uses:
o	clientId(App registration on AD)
o	tenantId
•	Authentication is done via Azure AD.
________________________________________
✅ Step 6: Retrieve Secrets from Key Vault
•	Secrets retrieved:
o	accountKey
o	accountName
o	containerName
•	Secrets are required to access Azure Blob Storage.
________________________________________
✅ Step 7: Connect to Azure Blob Storage
•	Using retrieved credentials, File-Manager establishes connection with Azure Blob Storage.
________________________________________
✅ Step 8: Store Summary File in Blob Storage
•	Upload summary.json into Azure Blob Storage:
o	Folder structure:
/containerName/{sourceSystem}/{batchId}/summary.json
________________________________________
✅ Step 9: Send Kafka Message to Output Topic
•	File-Manager sends a Kafka message to the output topic (str-ecp-batch-composition-complete):
o	Contains batchId, fileName, status, and blob URL.
________________________________________
✅ Step 10: Return Final API Response
•	File-Manager sends final REST API response:
o	Processing status (SUCCESS / FAILURE)
o	Summary file Blob URL
o	Kafka output message details
________________________________________



📚 Technologies Used
Component	Technology
Messaging	Apache Kafka (SSL enabled)
Document Processing	OpenText
Secrets Management	Azure Key Vault
Storage	Azure Blob Storage
Application Framework	Spring Boot
________________________________________
📊 Flow Diagram
Kafka Input Topic → File-Manager → Kafka Message to OpenText → OpenText Processes → API service call →
File-Manager → Prepare Summary.json → Connect to KeyVault → Get Secrets → Connect to Blob Storage →
Upload Summary File → Send Kafka Output Message → Return API Response
________________________________________
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

