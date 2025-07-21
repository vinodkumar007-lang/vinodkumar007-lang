 SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message,
                    processedFiles,      // ✅ Now includes blob URLs + status from buildDetailedProcessedFiles
                    pagesProcessed,
                    printFiles.toString(),
                    mobstatTriggerUrl,
                    customersProcessed
            );
