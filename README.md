
    public ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            List<BatchFile> dataFiles = message.getBatchFiles().stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).toList();
            message.setBatchFiles(dataFiles);

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : dataFiles) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            OTResponse otResponse = callOrchestrationBatchApi("eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiI1ZjFkMzFjNC02ZTdkLTRlYWEtOTU3MC1hMGY4OWJiOGI3NTUiLCJzYXQiOjE3NTIyNDU2NTcsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiIxZmQ2YmI4NC00YjY0LTQzZDgtOTJiMS1kY2U2YWIzZDQ3OWYiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODM3ODE2NTcsImlhdCI6MTc1MjI0NTY1NywianRpIjoiMGU4ZWI4NzYtOWJmYi00OTczLWFiN2ItM2EyZTg4NWM5N2MzIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JdXQ7pDNlEBS8jOny0yhKrC85CsypDdJzjww_OhVKL4BNBLQRfJf04ESqcnoONEIfbeARLGPS6THMP6K6xOeHcO7oViTFtgXg27jhrfj6OXiU52pAvo2qFBAs6VvTueNjDOyQMsau-PzigYdPNw86IWzeK0Ude7DhaR1rNTPbu7LsqKHM3aD6SFli0EeLSux5eJYdWqTy2gpH4iNodxPjlyt5i6UoNEwl1TqUwbMEtbztfrGiwMPXvSflGBH10pSDDtNpssiyvsDl_flnqLmqxso-Ff5AVs8eAjHgsQnSEIeQQp9sX0JoSbNgW8D0iACdlI-6f9onOLg4JW-Ozucmg", message);
            if (otResponse == null) return new ApiResponse("OT call failed", "error", null);

            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) return new ApiResponse("_STDDELIVERYFILE.xml not found", "error", null);

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            int customersProcessed = 0;
            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String val = outputList.getAttribute("customersProcessed");
                if (val != null) {
                    try {
                        customersProcessed = Integer.parseInt(val);
                    } catch (NumberFormatException ignored) {}
                }
            }

            String errorReportFilePath = null;
            NodeList reportFileNodes = doc.getElementsByTagName("reportFile");
            for (int i = 0; i < reportFileNodes.getLength(); i++) {
                Element reportFile = (Element) reportFileNodes.item(i);
                if ("Error_Report".equalsIgnoreCase(reportFile.getAttribute("dataFile"))) {
                    errorReportFilePath = reportFile.getAttribute("name");
                    break;
                }
            }

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);

            Map<String, String> successMap = new HashMap<>();
            for (SummaryProcessedFile s : processedFiles) successMap.put(s.getAccountNumber(), s.getCustomerId());
            processedFiles.addAll(appendFailureEntries(errorReportFilePath, successMap));

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

            // upload DropData.trigger to blob
            Path triggerFile = Paths.get(mobstatTriggerPath);
            if (Files.exists(triggerFile)) {
                String remotePath = String.format("%s/%s/%s/mobstat_trigger/DropData.trigger",
                        message.getSourceSystem(), message.getBatchId(), message.getUniqueConsumerRef());
                blobStorageService.uploadFile(triggerFile.toFile(), remotePath);
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, mobstatTriggerPath, customersProcessed);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("❌ Processing failed", ex);
            return new ApiResponse("Processing failed", "error", null);
        }
    }
