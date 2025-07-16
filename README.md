package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class CustomerSummary {
    private String customerId;
    private String accountNumber;
    private String cisNumber;
    private String status;
    private String pdfArchiveFileURL;
    private String pdfEmailFileURL;
    private String pdfMobstatFileURL;
    private String statusCode;
    private String statusDescription;
    private String printFileURL;
    private List<FileDetail> files;

    // ✅ Only used for debugging/logging; not included in summary.json
    @JsonIgnore
    private Map<String, String> deliveryStatus;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class FileDetail {
        private String objectId;
        private String fileLocation;
        private String fileUrl;
        private boolean encrypted;
        private String status;
        private String type;
    }
}

private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> map = new HashMap<>();
    Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");

    if (!Files.exists(errorPath)) return map;

    try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                String acc = parts[0].trim();
                String method = parts[3].trim().toUpperCase();
                String status = parts[4].trim();
                map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
            } else if (parts.length >= 3) {
                String acc = parts[0].trim();
                String method = parts[2].trim().toUpperCase();
                String status = parts.length > 3 ? parts[3].trim() : "Failed";
                map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
            }
        }
    } catch (Exception e) {
        logger.warn("⚠️ Error reading ErrorReport.csv", e);
    }
    return map;
}

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

                // Merge error report
                Map<String, String> deliveryStatus = errorMap.getOrDefault(acc, new HashMap<>());
                cs.setDeliveryStatus(deliveryStatus); // for logs only

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
        logger.error("❌ Failed parsing STD XML", e);
    }
    return list;
}

Map<String, Map<String, String>> errorMap = parseErrorReport(message);
List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
