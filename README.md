private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
    long startTime = System.currentTimeMillis();
    File xmlFile = null;

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPath = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                        .findFirst();

                if (xmlPath.isPresent()) {
                    xmlFile = xmlPath.get().toFile();

                    // ✅ Check file size is stable (not growing)
                    long size1 = xmlFile.length();
                    TimeUnit.SECONDS.sleep(1); // wait a second
                    long size2 = xmlFile.length();

                    if (size1 > 0 && size1 == size2) {
                        logger.info("✅ Found stable XML file: {}", xmlFile.getAbsolutePath());
                        return xmlFile;
                    } else {
                        logger.info("⌛ XML file still being written (size changing): {}", xmlFile.getAbsolutePath());
                    }
                }
            } catch (IOException e) {
                logger.warn("⚠️ Error scanning docgen folder", e);
            }
        } else {
            logger.debug("🔍 docgen folder not found yet: {}", docgenRoot);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    logger.error("❌ Timed out waiting for complete XML file in {}", docgenRoot);
    return null;
}
