private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
    long startTime = System.currentTimeMillis();
    File xmlFile = null;

    final int stableCheckCount = 3; // consecutive stable checks required
    int stableCounter = 0;

    long pollInterval = rptPollIntervalMillis; // initial poll interval in ms
    final long maxPollInterval = 15000L; // max interval 15 sec
    final double growthFactor = 1.5; // increase interval gradually if file is still growing

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPath = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                        .findFirst();

                if (xmlPath.isPresent()) {
                    xmlFile = xmlPath.get().toFile();

                    long size1 = xmlFile.length();
                    TimeUnit.MILLISECONDS.sleep(pollInterval);
                    long size2 = xmlFile.length();
                    TimeUnit.MILLISECONDS.sleep(pollInterval);
                    long size3 = xmlFile.length();

                    if (size1 > 0 && size1 == size2 && size2 == size3) {
                        stableCounter++;
                        if (stableCounter >= stableCheckCount) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                            return xmlFile;
                        } else {
                            logger.info("[{}] 📄 XML file appears stable ({}/{})", jobId, stableCounter, stableCheckCount);
                        }
                        // Reset poll interval for next stable check
                        pollInterval = rptPollIntervalMillis;
                    } else {
                        stableCounter = 0; // reset if not stable
                        logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());

                        // Increase poll interval gradually for large file growth
                        pollInterval = Math.min(maxPollInterval, (long)(pollInterval * growthFactor));
                        logger.debug("[{}] ⏱ Increasing poll interval to {} ms", jobId, pollInterval);
                    }
                }
            } catch (IOException e) {
                logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
            }
        } else {
            logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
        }

        TimeUnit.MILLISECONDS.sleep(pollInterval);
    }

    String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
    logger.error(errMsg);
    throw new IllegalStateException(errMsg);
}
