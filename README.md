private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
        long startTime = System.currentTimeMillis();
        File xmlFile = null;

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
                        TimeUnit.SECONDS.sleep(1);
                        long size2 = xmlFile.length();

                        if (size1 > 0 && size1 == size2) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                            return xmlFile;
                        } else {
                            logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());
                        }
                    }
                } catch (IOException e) {
                    logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
                }
            } else {
                logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
        logger.error(errMsg);
        throw new IllegalStateException(errMsg);
    }
