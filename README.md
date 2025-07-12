File xmlFile = waitForXmlFile(...);
if (xmlFile == null) return new ApiResponse("_STDDELIVERYFILE.xml not found", "error", null);

if (xmlFile.length() == 0) {
    logger.error("❌ _STDDELIVERYFILE.xml is empty");
    return new ApiResponse("_STDDELIVERYFILE.xml is empty", "error", null);
}
