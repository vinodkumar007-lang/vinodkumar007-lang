private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();

            NodeList customerNodes = document.getElementsByTagName("customer");

            for (int i = 0; i < customerNodes.getLength(); i++) {
                Element customerElement = (Element) customerNodes.item(i);

                String accountNumber = null;
                String cisNumber = null;
                List<String> deliveryMethods = new ArrayList<>();

                NodeList keyNodes = customerElement.getElementsByTagName("key");
                for (int j = 0; j < keyNodes.getLength(); j++) {
                    Element keyElement = (Element) keyNodes.item(j);
                    String keyName = keyElement.getAttribute("name");

                    if ("AccountNumber".equalsIgnoreCase(keyName)) {
                        accountNumber = keyElement.getTextContent();
                    } else if ("CISNumber".equalsIgnoreCase(keyName)) {
                        cisNumber = keyElement.getTextContent();
                    }
                }

                NodeList queueNodes = customerElement.getElementsByTagName("queueName");
                for (int q = 0; q < queueNodes.getLength(); q++) {
                    String method = queueNodes.item(q).getTextContent().trim().toUpperCase();
                    if (!method.isEmpty()) {
                        deliveryMethods.add(method);
                    }
                }

                if (accountNumber != null && cisNumber != null) {
                    CustomerSummary summary = new CustomerSummary();
                    summary.setAccountNumber(accountNumber);
                    summary.setCisNumber(cisNumber);
                    summary.setCustomerId(accountNumber);

                    Map<String, String> deliveryStatusMap = errorMap.getOrDefault(accountNumber, new HashMap<>());
                    summary.setDeliveryStatus(deliveryStatusMap);

                    long failedCount = deliveryMethods.stream()
                            .filter(method -> "FAILED".equalsIgnoreCase(deliveryStatusMap.getOrDefault(method, "")))
                            .count();

                    if (failedCount == deliveryMethods.size()) {
                        summary.setStatus("FAILED");
                    } else if (failedCount > 0) {
                        summary.setStatus("PARTIAL");
                    } else {
                        summary.setStatus("SUCCESS");
                    }

                    customerSummaries.add(summary);

                    logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                            accountNumber, cisNumber, deliveryMethods, failedCount, summary.getStatus());
                }
            }

        } catch (Exception e) {
            logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
            throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
        }

        return customerSummaries;
    }
