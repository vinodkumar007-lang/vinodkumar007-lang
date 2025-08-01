 // Decode and attach print file URLs
        List<PrintFile> printFileList = new ArrayList<>();
        for (PrintFile pf : printFiles) {
            if (pf.getPrintFileURL() != null) {
                String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);
                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(decodedUrl);
                printFileList.add(printFile);
            }
        }
        payload.setPrintFiles(printFileList);
