public static void appendToSummaryJson(File summaryFile, SummaryPayload newPayload, String azureBlobStorageAccount) {
    try {
        ObjectNode root;
        if (summaryFile.exists()) {
            root = (ObjectNode) mapper.readTree(summaryFile);
        } else {
            root = mapper.createObjectNode();
            root.put("batchID", newPayload.getHeader().getBatchId());
            root.put("fileName", "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv");

            // Populate header block
            ObjectNode headerNode = mapper.createObjectNode();
            headerNode.put("tenantCode", newPayload.getHeader().getTenantCode());
            headerNode.put("channelID", newPayload.getHeader().getChannelID());
            headerNode.put("audienceID", newPayload.getHeader().getAudienceID());
            headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
            headerNode.put("sourceSystem", newPayload.getHeader().getSourceSystem());
            headerNode.put("product", "DEBTMANAGER");
            headerNode.put("jobName", newPayload.getHeader().getJobName());
            root.set("header", headerNode);

            root.set("processedFiles", mapper.createArrayNode());
            root.set("printFiles", mapper.createArrayNode());
        }

        ArrayNode processedFiles = (ArrayNode) root.withArray("processedFiles");
        Set<String> existingCustomerIds = new HashSet<>();
        for (JsonNode node : processedFiles) {
            existingCustomerIds.add(node.get("customerID").asText());
        }

        if (newPayload.getMetaData().getCustomerSummaries() != null) {
            for (CustomerSummary customer : newPayload.getMetaData().getCustomerSummaries()) {
                if (!existingCustomerIds.contains(customer.getCustomerId())) {
                    ObjectNode custNode = mapper.createObjectNode();
                    custNode.put("customerID", customer.getCustomerId());
                    custNode.put("accountNumber", customer.getAccountNumber());
                    String acc = customer.getAccountNumber();
                    String batchId = newPayload.getHeader().getBatchId();
                    custNode.put("pdfArchiveFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/archive", acc, batchId, "pdf"));
                    custNode.put("pdfEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/email", acc, batchId, "pdf"));
                    custNode.put("htmlEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/html", acc, batchId, "html"));
                    custNode.put("txtEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/txt", acc, batchId, "txt"));
                    custNode.put("pdfMobstatFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", acc, batchId, "pdf"));
                    custNode.put("statusCode", "OK");
                    custNode.put("statusDescription", "Success");
                    processedFiles.add(custNode);
                    existingCustomerIds.add(customer.getCustomerId());
                }
            }
        }

        ArrayNode printFiles = (ArrayNode) root.withArray("printFiles");
        Set<String> existingPrintUrls = new HashSet<>();
        for (JsonNode node : printFiles) {
            existingPrintUrls.add(node.get("printFileURL").asText());
        }

        if (newPayload.getPayload().getPrintFiles() != null) {
            for (String pf : newPayload.getPayload().getPrintFiles()) {
                String url = buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", pf, newPayload.getHeader().getBatchId(), "ps");
                if (!existingPrintUrls.contains(url)) {
                    ObjectNode pfNode = mapper.createObjectNode();
                    pfNode.put("printFileURL", url);
                    printFiles.add(pfNode);
                    existingPrintUrls.add(url);
                }
            }
        }

        // Update header timestamp to latest (optional)
        ObjectNode headerNode = (ObjectNode) root.get("header");
        if (headerNode != null) {
            headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
        }

        mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, root);
        logger.info("Appended to summary.json: {}", summaryFile.getAbsolutePath());

    } catch (IOException e) {
        logger.error("Error appending to summary.json", e);
    }
}
