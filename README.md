private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
    Map<String, Integer> summaryCounts = new HashMap<>();
    try {
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
        doc.getDocumentElement().normalize();
        NodeList outputListNodes = doc.getElementsByTagName("outputList");
        if (outputListNodes.getLength() > 0) {
            Element outputList = (Element) outputListNodes.item(0);
            String customersProcessed = outputList.getAttribute("customersProcessed");
            String pagesProcessed = outputList.getAttribute("pagesProcessed");

            int custCount = customersProcessed != null && !customersProcessed.isBlank()
                    ? Integer.parseInt(customersProcessed) : 0;
            int pageCount = pagesProcessed != null && !pagesProcessed.isBlank()
                    ? Integer.parseInt(pagesProcessed) : 0;

            summaryCounts.put("customersProcessed", custCount);
            summaryCounts.put("pagesProcessed", pageCount);
        }
    } catch (Exception e) {
        logger.warn("⚠️ Unable to extract summary counts from XML", e);
    }
    return summaryCounts;
}

// ⬇️ Extract customersProcessed and pagesProcessed from XML <outputList>
Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);
int customersProcessed = summaryCounts.getOrDefault("customersProcessed", processedFiles.size());
int pagesProcessed = summaryCounts.getOrDefault("pagesProcessed", 0);

SummaryPayload payload = SummaryJsonWriter.buildPayload(
        message,
        processedFiles,
        printFiles,
        mobstatTriggerUrl,
        customersProcessed // ✅ now using extracted value
);

payload.setFileName(message.getBatchFiles().get(0).getFilename());
payload.setTimestamp(currentTimestamp);
payload.setTotalCustomersProcessed(customersProcessed); // ✅ new
payload.setTotalPagesProcessed(pagesProcessed);         // ✅ new
