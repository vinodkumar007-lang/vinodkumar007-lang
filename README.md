public static void appendToSummaryJson(File summaryFile, SummaryPayload newPayload, String azureBlobStorageAccount) {
    try {
        ObjectNode root;
        if (summaryFile.exists()) {
            root = (ObjectNode) mapper.readTree(summaryFile);
        } else {
            root = mapper.createObjectNode();
            root.put("batchID", newPayload.getHeader().getBatchId());
            root.put("fileName", "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv");
            root.set("header", mapper.createObjectNode());
            root.set("processedFiles", mapper.createArrayNode());
            root.set("printFiles", mapper.createArrayNode());
        }

        // Append processedFiles
        ArrayNode processedFiles = (ArrayNode) root.withArray("processedFiles");
        for (CustomerSummary customer : newPayload.getMetaData().getCustomerSummaries()) {
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
        }

        // Append print files
        ArrayNode printFiles = (ArrayNode) root.withArray("printFiles");
        for (String pf : newPayload.getPayload().getPrintFiles()) {
            ObjectNode pfNode = mapper.createObjectNode();
            pfNode.put("printFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", pf, newPayload.getHeader().getBatchId(), "ps"));
            printFiles.add(pfNode);
        }

        mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, root);
        logger.info("Appended to summary.json: {}", summaryFile.getAbsolutePath());

    } catch (IOException e) {
        logger.error("Error appending to summary.json", e);
    }
}
