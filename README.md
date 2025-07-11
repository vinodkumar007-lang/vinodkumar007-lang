private File waitForXmlFile(String jobId, String id) throws InterruptedException, IOException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
    int maxAttempts = 3;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        logger.info("🔁 Attempt {}/{}: Looking for _STDDELIVERYFILE.xml under {}", attempt, maxAttempts, docgenRoot);

        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (!Files.exists(docgenRoot)) {
                logger.info("❗ docgen folder not found. Retrying in {}ms...", rptPollIntervalMillis);
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                continue;
            }

            try (Stream<Path> flowstepDirs = Files.list(docgenRoot).filter(Files::isDirectory)) {
                Optional<Path> flowstepPathOpt = flowstepDirs.findFirst();

                if (flowstepPathOpt.isPresent()) {
                    Path flowstep = flowstepPathOpt.get();
                    Path outputPath = flowstep.resolve("output");

                    if (Files.exists(outputPath)) {
                        try (Stream<Path> files = Files.list(outputPath)) {
                            Optional<File> xmlFile = files
                                    .filter(Files::isRegularFile)
                                    .map(Path::toFile)
                                    .filter(f -> f.getName().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                                    .findFirst();

                            if (xmlFile.isPresent()) {
                                logger.info("✅ Found _STDDELIVERYFILE.xml in: {}", xmlFile.get().getAbsolutePath());
                                return xmlFile.get();
                            } else {
                                logger.info("🔎 Output folder found but XML not ready. Retrying...");
                            }
                        }
                    } else {
                        logger.info("🔎 Flowstep 'output' folder not ready yet. Retrying...");
                    }
                } else {
                    logger.info("⏳ No flowstep folder under docgen. Retrying...");
                }
            } catch (Exception e) {
                logger.error("❌ Error while scanning docgen", e);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        logger.warn("⌛ Timeout for attempt {}/{}. Retrying if allowed...", attempt, maxAttempts);
        TimeUnit.SECONDS.sleep(5);
    }

    logger.error("❌ All {} attempts failed. XML not found at path: jobs/{}/{}/docgen/*/output/", maxAttempts, jobId, id);
    return null;
}
