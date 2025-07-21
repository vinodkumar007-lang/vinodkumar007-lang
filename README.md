// ✅ Parse error report (already used in buildDetailedProcessedFiles)
Map<String, Map<String, String>> errorMap = parseErrorReport(xmlFile, message);
logger.info("🧾 Parsed error report with {} entries", errorMap.size());

// ✅ Parse STDXML and extract basic customer summaries
List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, message);
logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());
