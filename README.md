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
