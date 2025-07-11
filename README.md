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

        try (Stream<Path> subDirs = Files.list(docgenRoot).filter(Files::isDirectory)) {
            Optional<Path> immediateSubDir = subDirs.findFirst();
            if (immediateSubDir.isPresent()) {
                Path outputDir = immediateSubDir.get().resolve("output");
                if (Files.exists(outputDir)) {
                    try (Stream<Path> files = Files.list(outputDir)) {
                        Optional<File> xmlFile = files
                                .filter(Files::isRegularFile)
                                .map(Path::toFile)
                                .filter(f -> f.getName().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                                .findFirst();

                        if (xmlFile.isPresent()) {
                            logger.info("✅ Found _STDDELIVERYFILE.xml at: {}", xmlFile.get().getAbsolutePath());
                            return xmlFile.get();
                        } else {
                            logger.info("🕓 _STDDELIVERYFILE.xml not yet present in: {}", outputDir);
                        }
                    }
                } else {
                    logger.info("📁 output folder not found under: {}", immediateSubDir.get());
                }
            } else {
                logger.info("🕓 No folders yet inside docgen: {}", docgenRoot);
            }
        } catch (Exception e) {
            logger.error("⚠️ Error scanning docgen directory: {}", docgenRoot, e);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    logger.warn("❌ Timed out after {} seconds waiting for _STDDELIVERYFILE.xml in {}", rptMaxWaitSeconds, docgenRoot);
    return null;
}
