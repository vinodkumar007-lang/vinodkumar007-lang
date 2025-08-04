// ✅ Step 1: Assign status based on conditions before decoding
for (PrintFile pf : printFiles) {
    String psUrl = pf.getPrintFileURL();

    if (psUrl != null && psUrl.endsWith(".ps")) {
        // .ps file exists, set SUCCESS
        pf.setPrintStatus("SUCCESS");
    } else if (psUrl != null && errorMap.containsKey(psUrl)) {
        // Error found for this file
        pf.setPrintStatus("FAILED");
    } else {
        // Not found or unclear
        pf.setPrintStatus("");
    }
}

// ✅ Step 2: Decode and collect into final printFileList
List<PrintFile> printFileList = new ArrayList<>();

for (PrintFile pf : printFiles) {
    if (pf.getPrintFileURL() != null) {
        String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(decodedUrl);
        printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

        printFileList.add(printFile);
    }
}

// ✅ Step 3: Set in payload
payload.setPrintFiles(printFileList);
