private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
    logger.info("🔍 Looking for _STDDELIVERYFILE.xml under {}", docgenRoot);

    long startTime = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (!Files.exists(docgenRoot)) {
            logger.info("📂 docgen folder not yet available. Retrying in {}ms...", rptPollIntervalMillis);
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            continue;
        }

        try (Stream<Path> paths = Files.walk(docgenRoot)) {
            Optional<Path> xmlPath = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                    .findFirst();

            if (xmlPath.isPresent()) {
                File xmlFile = xmlPath.get().toFile();

                // Check non-empty
                if (xmlFile.length() == 0) {
                    logger.info("⏳ XML file found but still empty. Waiting...");
                    TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                    continue;
                }

                // Try parsing to ensure it’s complete
                try {
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    dBuilder.parse(xmlFile);  // Attempt parse
                    logger.info("✅ Valid and complete XML file found: {}", xmlFile.getAbsolutePath());
                    return xmlFile;
                } catch (Exception e) {
                    logger.info("⏳ XML file found but still being written (not parseable). Waiting...");
                    TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                }
            }
        } catch (IOException e) {
            logger.warn("⚠️ Error while scanning docgen folder", e);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    logger.error("❌ Timed out after {} seconds waiting for complete _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
    return null;
}
