private File waitForXmlFile(String jobId, String id) throws InterruptedException, IOException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
        logger.info("🔎 Searching for _STDDELIVERYFILE.xml in {}", docgenRoot);

        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (!Files.exists(docgenRoot)) {
                logger.info("Waiting for docgen folder... retrying in {}ms", rptPollIntervalMillis);
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                continue;
            }

            try (Stream<Path> flowstepDirs = Files.list(docgenRoot).filter(Files::isDirectory)) {
                Optional<Path> flowstep = flowstepDirs.findFirst();
                if (flowstep.isPresent()) {
                    Path outputDir = flowstep.get().resolve("output");
                    if (Files.exists(outputDir)) {
                        try (Stream<Path> files = Files.list(outputDir)) {
                            Optional<File> xmlFile = files
                                    .filter(Files::isRegularFile)
                                    .map(Path::toFile)
                                    .filter(f -> f.getName().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                                    .findFirst();

                            if (xmlFile.isPresent()) {
                                logger.info("✅ Found XML file: {}", xmlFile.get().getAbsolutePath());
                                return xmlFile.get();
                            }
                        }
                    }
                }
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        logger.warn("❌ Timed out after {} seconds while searching for _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
        return null;
    }
