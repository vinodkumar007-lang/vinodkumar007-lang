private static List<PrintFile> processPrintFiles(List<PrintFile> printFiles, Map<String, Map<String, String>> errorMap, String batchId, String fileName) {
    List<PrintFile> result = new ArrayList<>();

    for (PrintFile pf : printFiles) {
        if (pf == null) {
            logger.debug("[buildPayload] Skipping null PrintFile. batchId={}, fileName={}", batchId, fileName);
            continue;
        }

        String psUrl = pf.getPrintFileURL();

        // ✅ Only include .ps files
        if (psUrl == null || !psUrl.toLowerCase().endsWith(".ps")) {
            logger.debug("[buildPayload] Skipping non-.ps PrintFile. batchId={}, fileName={}, url={}", batchId, fileName, psUrl);
            continue;
        }

        String decodedUrl = URLDecoder.decode(psUrl, StandardCharsets.UTF_8);

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(decodedUrl);
        printFile.setPrintStatus("SUCCESS"); // Set status SUCCESS by default; adjust if errorMap check needed

        result.add(printFile);

        logger.debug("[GT] PrintFile processed. batchId={}, fileName={}, psUrl={}, status={}",
                batchId, fileName, decodedUrl, printFile.getPrintStatus());
    }

    return result;
}
