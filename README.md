private File waitForXmlFile(String jobId, String id, int expectedRecords) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
    long startTime = System.currentTimeMillis();
    File xmlFile = null;

    final int stableChecksRequired = 3; // require N consecutive stable measurements
    final int sizeHistoryCount = 5;     // track last 5 sizes
    final long growthTolerance = 1024;  // 1 KB tolerance

    Deque<Long> recentSizes = new ArrayDeque<>();
    Deque<Long> recentModifiedTimes = new ArrayDeque<>();

    long currentPollInterval = rptPollIntervalMillis; // start poll interval
    final long maxPollInterval = 30000;               // max 30 sec
    final long minPollInterval = 2000;                // min 2 sec

    int stableCounter = stableChecksRequired;

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPathOpt = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                        .findFirst();

                if (xmlPathOpt.isPresent()) {
                    xmlFile = xmlPathOpt.get().toFile();
                    long currentSize = xmlFile.length();
                    long currentModified = xmlFile.lastModified();

                    // Add to history
                    if (recentSizes.size() >= sizeHistoryCount) recentSizes.pollFirst();
                    recentSizes.addLast(currentSize);

                    if (recentModifiedTimes.size() >= sizeHistoryCount) recentModifiedTimes.pollFirst();
                    recentModifiedTimes.addLast(currentModified);

                    // Check if all recent measurements are stable
                    boolean stableSize = recentSizes.stream()
                            .allMatch(size -> Math.abs(size - currentSize) <= growthTolerance)
                            && recentModifiedTimes.stream().allMatch(mod -> mod == currentModified);

                    // Check record count
                    int recordCount = countXmlRecords(xmlFile); // implement a simple record counter
                    boolean recordCountMatch = recordCount >= expectedRecords;

                    if (stableSize && recordCountMatch && currentSize > 0) {
                        stableCounter--;
                        if (stableCounter <= 0) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML + " with {} records", xmlFile.getAbsolutePath(), recordCount);
                            return xmlFile;
                        }
                        currentPollInterval = Math.min(currentPollInterval + 1000, maxPollInterval);
                    } else {
                        stableCounter = stableChecksRequired; // reset counter if not stable
                        currentPollInterval = minPollInterval;
                        logger.info(AppConstants.LOG_XML_SIZE_CHANGING + ", current records={}", xmlFile.getAbsolutePath(), recordCount);
                    }
                }
            } catch (IOException e) {
                logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
            }
        } else {
            logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
        }

        TimeUnit.MILLISECONDS.sleep(currentPollInterval);
    }

    String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
    logger.error(errMsg);
    throw new IllegalStateException(errMsg);
}

// Simple method to count XML records (lines with <record> tag)
private int countXmlRecords(File xmlFile) {
    int count = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(xmlFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().startsWith("<record")) { // adjust tag based on your XML
                count++;
            }
        }
    } catch (IOException e) {
        logger.warn("Failed to count XML records in file {}: {}", xmlFile.getAbsolutePath(), e.getMessage());
    }
    return count;
}
