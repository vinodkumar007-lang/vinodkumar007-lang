private List<CustomerSummary> parseAndUploadPrintFiles(File xmlFile) {
    List<CustomerSummary> list = new ArrayList<>();

    try {
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList queueNodes = doc.getElementsByTagName("queue");

        for (int i = 0; i < queueNodes.getLength(); i++) {
            Element queueElement = (Element) queueNodes.item(i);
            String queueName = queueElement.getAttribute("name");

            if (!"print".equalsIgnoreCase(queueName)) continue;

            NodeList fileNodes = queueElement.getElementsByTagName("file");

            for (int j = 0; j < fileNodes.getLength(); j++) {
                Element fileElement = (Element) fileNodes.item(j);
                String filePath = fileElement.getAttribute("name");

                // Upload the print file to blob and get URL
                String printBlobUrl = uploadFileToBlob(filePath);

                NodeList customerNodes = fileElement.getElementsByTagName("customer");

                for (int k = 0; k < customerNodes.getLength(); k++) {
                    Element customerElement = (Element) customerNodes.item(k);
                    String accountNumber = "";
                    String cisNumber = "";
                    String customerId = customerElement.getAttribute("number");

                    NodeList keyNodes = customerElement.getElementsByTagName("key");
                    for (int m = 0; m < keyNodes.getLength(); m++) {
                        Element keyElement = (Element) keyNodes.item(m);
                        String keyName = keyElement.getAttribute("name");
                        String keyValue = keyElement.getTextContent();

                        if ("AccountNumber".equals(keyName)) accountNumber = keyValue;
                        if ("CISNumber".equals(keyName)) cisNumber = keyValue;
                    }

                    CustomerSummary cs = new CustomerSummary();
                    cs.setCustomerId(customerId);
                    cs.setAccountNumber(accountNumber);
                    cs.setCisNumber(cisNumber);
                    cs.setStatus("SUCCESS");
                    cs.setPrintFileURL(printBlobUrl);

                    list.add(cs);

                    logger.debug("✅ Uploaded for customer {}, printBlob={}", customerId, printBlobUrl);
                }
            }
        }

    } catch (Exception e) {
        logger.error("❌ Failed to parse and upload print files from XML", e);
    }

    return list;
}

List<CustomerSummary> printCustomers = parseAndUploadPrintFiles(stdliXmlFile);
