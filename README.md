private Map<String, Set<String>> parseSTDXml(Path stdFilePath) {
    Map<String, Set<String>> requestedOutputMethods = new HashMap<>();

    try {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(stdFilePath.toFile());
        doc.getDocumentElement().normalize();

        NodeList queueList = doc.getElementsByTagName("queue");

        for (int i = 0; i < queueList.getLength(); i++) {
            Node queueNode = queueList.item(i);
            if (queueNode.getNodeType() == Node.ELEMENT_NODE) {
                Element queueElement = (Element) queueNode;

                String method = queueElement.getAttribute("name").toLowerCase(); // email, archive, print, mobstat
                Node parent = queueNode.getParentNode(); // should be document/recipient
                while (parent != null && !(parent instanceof Element && ((Element) parent).hasAttribute("accountNumber"))) {
                    parent = parent.getParentNode();
                }

                if (parent != null && parent.getNodeType() == Node.ELEMENT_NODE) {
                    Element recipient = (Element) parent;
                    String customerId = recipient.getAttribute("customerId");
                    String accountNumber = recipient.getAttribute("accountNumber");

                    if (!customerId.isEmpty() && !accountNumber.isEmpty()) {
                        String key = customerId + "|" + accountNumber;
                        requestedOutputMethods
                            .computeIfAbsent(key, k -> new HashSet<>())
                            .add(method.toUpperCase()); // normalize to uppercase
                    }
                }
            }
        }

    } catch (Exception e) {
        log.error("Error parsing STD XML: {}", e.getMessage(), e);
    }

    return requestedOutputMethods;
}


==============

private Map<String, Set<String>> parseErrorReport(Path errorReportPath) {
    Map<String, Set<String>> failedOutputMethods = new HashMap<>();

    try (BufferedReader reader = Files.newBufferedReader(errorReportPath)) {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                String customerId = parts[0].trim();
                String accountNumber = parts[1].trim();
                String failedMethod = parts[3].trim().toUpperCase(); // e.g., EMAIL, MOBSTAT

                // Normalize ARC suffix if any
                accountNumber = accountNumber.replaceAll("_ARC$", "");

                String key = customerId + "|" + accountNumber;
                failedOutputMethods
                    .computeIfAbsent(key, k -> new HashSet<>())
                    .add(failedMethod);
            }
        }
    } catch (IOException e) {
        log.error("Error reading ErrorReport: {}", e.getMessage(), e);
    }

    return failedOutputMethods;
}


======================
public static List<ProcessedFileEntry> buildProcessedFileEntries(
    List<SummaryProcessedFile> processedList,
    Map<String, Set<String>> stdRequestedMap,
    Map<String, String> errorReportMap
) {
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .collect(Collectors.groupingBy(spf -> spf.getCustomerId() + "::" + spf.getAccountNumber()));

    List<ProcessedFileEntry> result = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String customerKey = entry.getKey();
        List<SummaryProcessedFile> list = entry.getValue();

        Set<String> requestedMethods = stdRequestedMap.getOrDefault(customerKey, new HashSet<>());
        Map<String, String> typeToStatus = new HashMap<>();

        // Collect status for each type present
        for (SummaryProcessedFile spf : list) {
            String type = spf.getLinkedDeliveryType();
            typeToStatus.put(type, spf.getStatus());
        }

        Set<String> successSet = new HashSet<>();
        Set<String> failedSet = new HashSet<>();
        Set<String> missingSet = new HashSet<>();

        for (String reqType : requestedMethods) {
            String status = typeToStatus.get(reqType);
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successSet.add(reqType);
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedSet.add(reqType);
            } else {
                // Not present or not found
                missingSet.add(reqType);
            }
        }

        // Now handle missing types using ErrorReport
        for (String missing : new HashSet<>(missingSet)) {
            String errVal = errorReportMap.get(customerKey);
            if (errVal != null && errVal.equalsIgnoreCase(missing)) {
                failedSet.add(missing);
            } else {
                // Not in error report either → Partial
                // Just leave it in missingSet
            }
        }

        // Compute final status
        String finalStatus;
        if (successSet.containsAll(requestedMethods)) {
            finalStatus = "SUCCESS";
        } else if (failedSet.containsAll(requestedMethods)) {
            finalStatus = "FAILED";
        } else {
            finalStatus = "PARTIAL";
        }

        // Build processed entry
        SummaryProcessedFile sample = list.get(0);
        ProcessedFileEntry processed = new ProcessedFileEntry();
        processed.setCustomerId(sample.getCustomerId());
        processed.setAccountNumber(sample.getAccountNumber());
        processed.setStatus(finalStatus);
        processed.setFiles(list);
        result.add(processed);
    }

    return result;
}
