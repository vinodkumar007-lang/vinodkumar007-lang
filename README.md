List<CustomerSummary> customerSummaries = newPayload.getMetaData().getCustomerSummaries();
if (customerSummaries != null) {
    for (CustomerSummary customer : customerSummaries) {
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
}
