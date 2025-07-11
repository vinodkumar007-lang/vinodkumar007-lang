private File waitForXmlFile(String jobId, String id) throws InterruptedException, IOException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
    logger.info("🔎 Searching for _STDDELIVERYFILE.xml under {}", docgenRoot);

    long startTime = System.currentTimeMillis();
    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (!Files.exists(docgenRoot)) {
            logger.info("📂 docgen folder not found. Retrying in {}ms...", rptPollIntervalMillis);
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            continue;
        }

        // Recursively walk all subdirectories under docgen/
        try (Stream<Path> paths = Files.walk(docgenRoot)) {
            Optional<Path> xmlPath = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                    .findFirst();

            if (xmlPath.isPresent()) {
                File foundFile = xmlPath.get().toFile();
                logger.info("✅ Found XML file: {}", foundFile.getAbsolutePath());
                return foundFile;
            }
        } catch (Exception e) {
            logger.error("❌ Error while walking docgen directory", e);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    logger.warn("❌ Timed out after {} seconds while searching for _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
    return null;
}
