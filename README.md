List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
payload.setProcessedFileList(processedFileEntries);

// ✅ Set final file count as number of entries added to summary
payloadInfo.setFileCount(processedFileEntries.size());
