{
    "message": "Batch processed successfully",
    "status": "success",
    "summaryPayload": {
        "batchID": "12345",
        "header": {
            "tenantCode": "ZANBL",
            "channelID": "100",
            "audienceID": "",
            "timestamp": "Thu May 22 05:44:21 SAST 2025",
            "sourceSystem": "DEBTMAN",
            "product": "DEBTMANAGER",
            "jobName": "DEBTMAN"
        },
        "metadata": {
            "totalFilesProcessed": 1,
            "processingStatus": "200",
            "eventOutcomeCode": "Sucess",
            "eventOutcomeDescription": "Successful"
        },
        "payload": {
            "uniqueConsumerRef": "",
            "uniqueECPBatchRef": "",
            "runPriority": "",
            "eventID": "200",
            "eventType": "Success",
            "restartKey": ""
        },
        "summaryFileURL": "/main/nedcor/dia/ecm-batch/debtman/output/guid/exstreamsummaryfile/batchID_summary_sample.json",
        "timestamp": ""
    }
}

summary.json

{
  "batchID": "759af791-99fe-4a1b-a6de-ca06b2754c46",
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
	  "accountNumber": "5898460773955802",
      "pdfArchiveFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/archive/5898460773955802_CCEML805.pdf",
      "pdfEmailFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/email/5898460773955802_CCEML805.pdf",
      "htmlEmailFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/html/DM12_generic_RB.html",
      "pdfMobstatFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/mobstat/DEBTMAN_5898460773906474_600006708419_0999392815_0801114949_30_CARD.pdf",
	  "statusCode": "OK",
	  "statusDescription": "Success"
    },
    {
      "customerID": "C002",
	  "accountNumber": "5898460773869078"
      "pdfArchiveFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/archive/5898460773906474_CCMOB805.pdf",
      "pdfEmailFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/email/5898460773869078_CCEML805.pdf",
      "htmlEmailFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/html/DM12_generic_RB.html",
      "pdfMobstatFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/mobstat/DEBTMAN_1165371100101_146311653711_0822559186_0861100033_30_RRB.pdf",
	  "statusCode": "OK",
	  "statusDescription": "Success"
    }
  ],
  "printFiles": [
	{
      "printFileURL": "/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/print/MOBSTAT_PRINT.ps"		
	}
  ]
}
