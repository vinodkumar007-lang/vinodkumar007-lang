private static String getStatusByMethod(ProcessedFileEntry pf, String method) {
    switch (method.toUpperCase()) {
        case "EMAIL":
            return pf.getPdfEmailFileUrlStatus();
        case "ARCHIVE":
            return pf.getPdfArchiveFileUrlStatus();
        case "MOBSTAT":
            return pf.getPdfMobstatFileUrlStatus();
        case "PRINT":
            return pf.getPrintFileUrlStatus();
        default:
            return null;
    }
}
