SummaryProcessedFile summaryProcessedFile = new SummaryProcessedFile();
summaryProcessedFile.setFileName(outputFileName);
// 👇 🔴 MISSING LINE
summaryProcessedFile.setBlobURL(blobUrl); // ✅ ADD THIS LINE
summaryProcessedFile.setStatus(status);
summaryProcessedFile.setOutputMethod(outputMethod);
summaryProcessedFile.setCustomerId(customerId);
summaryProcessedFile.setFirstFailureReason(errorReason);
customerList.add(summaryProcessedFile);
