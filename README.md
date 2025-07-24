public List<PrintFileEntry> parsePrintQueueXml(File xmlFile, String mountPath) {
        List<PrintFileEntry> printFiles = new ArrayList<>();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList queueList = doc.getElementsByTagName("queue");

            for (int i = 0; i < queueList.getLength(); i++) {
                Element queueElement = (Element) queueList.item(i);
                String queueName = queueElement.getAttribute("name");

                // Only process <queue name="print">
                if (!"print".equalsIgnoreCase(queueName)) continue;

                NodeList fileList = queueElement.getElementsByTagName("file");

                for (int j = 0; j < fileList.getLength(); j++) {
                    Element fileElement = (Element) fileList.item(j);
                    String filePath = fileElement.getAttribute("name");

                    File localFile = new File(filePath); // absolute path already includes mount
                    if (!localFile.exists()) {
                        System.err.println("Print file not found: " + filePath);
                        continue;
                    }

                    String targetPath = "print/" + LocalDate.now() + "/" + localFile.getName();
                    String printBlobUrl = blobStorageService.uploadFile(localFile, targetPath);
                    logger.info("Print pdf blob post uploaded{}", printBlobUrl);
                    NodeList customerNodes = fileElement.getElementsByTagName("customer");

                    for (int k = 0; k < customerNodes.getLength(); k++) {
                        Element customerElement = (Element) customerNodes.item(k);
                        String customerNumber = customerElement.getAttribute("number");

                        Map<String, String> keys = new HashMap<>();
                        NodeList keyList = customerElement.getElementsByTagName("key");
                        for (int l = 0; l < keyList.getLength(); l++) {
                            Element keyElement = (Element) keyList.item(l);
                            keys.put(keyElement.getAttribute("name"), keyElement.getTextContent());
                        }

                        // Build the DTO
                        PrintFileEntry entry = new PrintFileEntry();
                        entry.setCustomerNumber(customerNumber);
                        entry.setPrintBlobPSUrl(printBlobUrl);
                        entry.setFileName(localFile.getName());
                        entry.setAccountNumber(keys.get("AccountNumber"));
                        entry.setCisNumber(keys.get("CISNumber"));
                        entry.setStartPage(keys.get("StartPageCustomer"));
                        entry.setTotalPages(keys.get("TotalPagesInDocument"));
                        entry.setPrintStatus("SUCCESS");
                        printFiles.add(entry);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return printFiles;
    }
