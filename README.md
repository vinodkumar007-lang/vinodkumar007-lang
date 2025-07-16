 private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
        List<CustomerSummary> list = new ArrayList<>();
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList customers = doc.getElementsByTagName("customer");
            for (int i = 0; i < customers.getLength(); i++) {
                Element cust = (Element) customers.item(i);

                String acc = null, cis = null;
                List<String> methods = new ArrayList<>();

                NodeList keys = cust.getElementsByTagName("key");
                for (int j = 0; j < keys.getLength(); j++) {
                    Element k = (Element) keys.item(j);
                    if ("AccountNumber".equalsIgnoreCase(k.getAttribute("name"))) acc = k.getTextContent();
                    if ("CISNumber".equalsIgnoreCase(k.getAttribute("name"))) cis = k.getTextContent();
                }

                NodeList queues = cust.getElementsByTagName("queueName");
                for (int q = 0; q < queues.getLength(); q++) {
                    String val = queues.item(q).getTextContent().trim().toUpperCase();
                    if (!val.isEmpty()) methods.add(val);
                }

                if (acc != null && cis != null) {
                    CustomerSummary cs = new CustomerSummary();
                    cs.setAccountNumber(acc);
                    cs.setCisNumber(cis);
                    cs.setCustomerId(acc);

                    // Merge error report
                    Map<String, String> deliveryStatus = errorMap.getOrDefault(acc, new HashMap<>());
                    cs.setDeliveryStatus(deliveryStatus); // for logs only

                    long failed = methods.stream()
                            .filter(m -> "FAILED".equalsIgnoreCase(deliveryStatus.getOrDefault(m, "")))
                            .count();

                    if (failed == methods.size()) {
                        cs.setStatus("FAILED");
                    } else if (failed > 0) {
                        cs.setStatus("PARTIAL");
                    } else {
                        cs.setStatus("SUCCESS");
                    }

                    list.add(cs);

                    logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                            acc, cis, methods, failed, cs.getStatus());
                }
            }
        } catch (Exception e) {
            logger.error("❌ Failed parsing STD XML", e);
        }
        return list;
    }


    public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        // Header block
        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        // 👇 Don't set root-level timestamp here; will be set in KafkaListenerService
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        // Determine overall status
        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));

            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        // Metadata block
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        // Payload block
        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(processedFiles != null ? processedFiles.size() : 0);
        payload.setPayload(payloadDetails);

        // 👇 Optional: include processed and print files in summary.json
        payload.setProcessedFiles(processedFiles != null ? processedFiles : new ArrayList<>());
        payload.setPrintFiles(printFiles != null ? printFiles : new ArrayList<>());

        return payload;
    }
