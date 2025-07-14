// Add failed entries from error report
Path errorReportPath = Paths.get(mountPath, "jobs", otResponse.getJobId(), otResponse.getId(), "error", "ErrorReport.txt");
List<SummaryProcessedFile> failures = appendFailureEntries(errorReportPath.toString(), accountCustomerMap);
processedFiles.addAll(failures);
