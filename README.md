private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPath = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                            .findFirst();
                    if (xmlPath.isPresent()) return xmlPath.get().toFile();
                } catch (IOException e) {
                    logger.warn("⚠️ Error scanning docgen", e);
                }
            }
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }
        return null;
    }
