private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
    long startTime = System.currentTimeMillis();
    File xmlFile = null;

    int stableCounter = 0;
    long pollInterval = rptPollIntervalMillis; // initial poll interval
    final long maxPollInterval = 15000L;       // max 15 sec
    final double growthFactor = 1.5;           // adaptive interval increase

    // --- dynamic stable check based on file size ---
    int baseStableCount = 3; // for small files (<5MB)
    final long smallFileThreshold = 5 * 1024 * 1024;   // 5 MB
    final long largeFileThreshold = 50 * 1024 * 1024;  // 50 MB
    int stableCheckCount = baseStableCount;

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPathOpt = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                        .findFirst();

                if (xmlPathOpt.isPresent()) {
                    xmlFile = xmlPathOpt.get().toFile();
                    long fileSize = xmlFile.length();

                    // --- dynamically adjust stableCheckCount ---
                    if (fileSize <= smallFileThreshold) stableCheckCount = 3;
                    else if (fileSize <= largeFileThreshold) stableCheckCount = 4;
                    else stableCheckCount = 6; // very large files

                    long size1 = fileSize;
                    TimeUnit.MILLISECONDS.sleep(pollInterval);
                    long size2 = xmlFile.length();
                    TimeUnit.MILLISECONDS.sleep(pollInterval);
                    long size3 = xmlFile.length();

                    if (size1 > 0 && size1 == size2 && size2 == size3) {
                        stableCounter++;
                        logger.info("[{}] 📄 XML file appears stable ({}/{}) size={} MB",
                                jobId, stableCounter, stableCheckCount, size1 / (1024 * 1024));

                        if (stableCounter >= stableCheckCount) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                            return xmlFile;
                        }

                        pollInterval = rptPollIntervalMillis; // reset interval
                    } else {
                        stableCounter = 0; // reset if file still growing
                        logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());

                        // gradually increase poll interval for large files
                        pollInterval = Math.min(maxPollInterval, (long) (pollInterval * growthFactor));
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
