for (PrintFile pf : printFiles) {
    String psUrl = pf.getPrintFileURL();
    if (psUrl != null && psUrl.endsWith(".ps")) {
        boolean exists = blobStorageService.doesBlobExist(psUrl); // your method to check
        if (exists) {
            pf.setPrintStatus("SUCCESS");
        } else if (errorMap.containsKey(psUrl)) {
            pf.setPrintStatus("FAILED");
        } else {
            pf.setPrintStatus("");
        }
    }
}
