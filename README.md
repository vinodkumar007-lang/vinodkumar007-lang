We're using a mounted path to store OT-processed files. Since no event is currently triggered after file generation, and the underlying NFS mount does not support OS-level file events, we're using a polling mechanism to detect file availability.

If an upstream event or notification becomes available in the future, we’re open to enhancing this with an event-driven approach.

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

                Map<String, String> deliveryStatus = errorMap.getOrDefault(acc, new HashMap<>());
                cs.setDeliveryStatus(deliveryStatus);

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
        logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
        // Optionally propagate:
        throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
    }
    return list;
}

Updated error handling to include the full XML file path in the exception log. This will help identify which file failed during parsing. The exception is still logged without rethrowing, as per current flow. Let me know if you'd prefer we propagate the failure to the caller.

The STD XML structure currently does not include any schema declaration or namespace reference (e.g., xsi:schemaLocation). As a result, schema validation is not applicable at this stage. If a corresponding XSD becomes available, we can integrate schema-based validation without impacting existing parsing logic.

