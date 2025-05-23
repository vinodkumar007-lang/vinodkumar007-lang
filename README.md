{
  "batchID": "12345",
  "fileName": "DEBTMAN_20250505.csv",
  "header": {
    "tenantCode": "ZANBL",
    "channelID": "100",
    "audienceID": "f7359b3f-4d8f-41a5-8df5-84b115cd8a74",
    "timestamp": "2025-02-06T12:34:56Z",
    "sourceSystem": "CARD",
    "product": "DEBTMANAGER",
    "jobName": "DEBTMAN"
  },
  "processedFiles": [
    {
      "customerID": "C001",
	  "accountNumber": "123456780123456",
      "pdfArchiveFileURL": "https://<azure_blob_storage_account>/pdfs/archive/123456780123456_12345.pdf",
      "pdfEmailFileURL": "https://<azure_blob_storage_account>/pdfs/email/123456780123456_12345.pdf",
      "htmlEmailFileURL": "https://<azure_blob_storage_account>/pdfs/html/123456780123456_12345.html",
      "txtEmailFileURL": "https://<azure_blob_storage_account>/pdfs/txt/123456780123456_12345.txt"
      "pdfMobstatFileURL": "https://<azure_blob_storage_account>/pdfs/mobstat/123456780123456_12345.pdf",
	  "statusCode": "OK",
	  "statusDescription": "Success"
    },
    {
      "customerID": "C002",
	  "accountNumber": "123456780654321"
      "pdfArchiveFileURL": "https://<azure_blob_storage_account>/pdfs/archive/123456780654321_12345.pdf",
      "pdfEmailFileURL": "https://<azure_blob_storage_account>/pdfs/email/123456780654321_12345.pdf",
      "htmlEmailFileURL": "https://<azure_blob_storage_account>/pdfs/html/123456780654321_12345.html",
      "txtEmailFileURL": "https://<azure_blob_storage_account>/pdfs/txt/123456780654321_12345.txt"
      "pdfMobstatFileURL": "https://<azure_blob_storage_account>/pdfs/mobstat/123456780654321_12345.pdf",
	  "statusCode": "OK",
	  "statusDescription": "Success"
    }
  ],
  "printFiles": [
	{
      "printFileURL": "https://<azure_blob_storage_account>/pdfs/mobstat/PrintFileName1_12345.ps"		
	},
	{
	  "printFileURL": "https://<azure_blob_storage_account>/pdfs/mobstat/PrintFileName2_12345.ps"		
	},
	  "printFileURL": "https://<azure_blob_storage_account>/pdfs/mobstat/PrintFileName3_12345.ps"		
	},
  ],
}
