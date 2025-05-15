Summary File Structure

• {

• "BatchID": "12345",

• "Header": {

• "TenantCode": "ZANBL", "ChannelID": "100", "AudienceID": "f7359b3f-4d8f-41a5-8df5-84b115cd8a74", "Timestamp": "2025-02-06T12:34:56Z", "SourceSystem": "CARD",

• "Product": "CASA", "JobName": "SMM815"

• },

• "Metadata": {

• "TotalFilesProcessed": 2, "ProcessingStatus": "Success", "EventOutcomeCode": "Success", "EventOutcomeDescription": "All customer PDFs processed successfully"

• },

• "Payload": {

• "UniqueConsumerRef": "d9e5ff0f-f237-4b8f-bf9f-e3c4bbf6c54c", "UniqueECPBatchRef": "C044A38E-0000-C21B-B1E2-69FEE895A17B", "FilenetObjectID": [ "C044A38E-0000-C21B-B1E2-69FEE895A17B", "D8EFC5A4-0000-B22A-B3D5-74FEE895A17B" ], "RepositoryID": "Legacy", "RunPriority": "High", "EventID": "E12345", "EventType": "Completion", "RestartKey": "Key12345"

• },

• "ProcessedFiles": [

• { "CustomerID": "C001", "PDFFileURL": "https://<azure_blob_storage_account>/pdfs/C001_12345.pdf" }, { "CustomerID": "C002", "PDFFileURL": "https://<azure_blob_storage_account>/pdfs/C002_12345.pdf"

• }

• ],

• "SummaryFileURL": "https:///summary/12345_summary.json",

• "Timestamp": "2025-02-06T12:34:56Z" }

=======================================
