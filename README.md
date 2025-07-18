String targetPath = String.format(
    "%s/%s/%s/%s/%s",                     // sourceSystem/batchId/guid/folder/filename
    msg.getSourceSystem(),
    msg.getBatchId(),
    msg.getGuid(),                        // Assuming msg.getGuid() gives the required GUID
    folder,
    fileName
);

String errorReportBlobPath = String.format(
    "%s/%s/%s/report/ErrorReport.txt",
    msg.getSourceSystem(),
    msg.getBatchId(),
    msg.getGuid()
);
