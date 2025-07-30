private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
    Map<String, Integer> summaryCounts = new HashMap<>();

    if (xmlFile == null || !xmlFile.exists() || !xmlFile.canRead()) {
        logger.warn("⚠️ Invalid or unreadable XML file: {}", xmlFile);
        return summaryCounts;
    }

    try {
        Document doc = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList outputListNodes = doc.getElementsByTagName("outputList");
        if (outputListNodes.getLength() > 0) {
            Element outputList = (Element) outputListNodes.item(0);
            String customersProcessed = outputList.getAttribute("customersProcessed");
            String pagesProcessed = outputList.getAttribute("pagesProcessed");

            int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                    ? Integer.parseInt(customersProcessed.trim()) : 0;
            int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                    ? Integer.parseInt(pagesProcessed.trim()) : 0;

            summaryCounts.put("customersProcessed", custCount);
            summaryCounts.put("pagesProcessed", pageCount);

            logger.info("📄 Extracted summary counts from {}: customersProcessed={}, pagesProcessed={}",
                        xmlFile.getName(), custCount, pageCount);
        } else {
            logger.info("ℹ️ No <outputList> found in XML: {}", xmlFile.getName());
        }
    } catch (Exception e) {
        logger.warn("⚠️ Unable to extract summary counts from XML file: {}", xmlFile.getName(), e);
    }

    return summaryCounts;
}
