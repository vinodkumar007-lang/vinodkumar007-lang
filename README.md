private File waitForXmlFile(Path docgenRoot) throws InterruptedException, IOException {
    logger.info("Waiting for _STDDELIVERYFILE.xml under: {}", docgenRoot);
    long startTime = System.currentTimeMillis();

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (!Files.exists(docgenRoot)) {
            logger.info("docgen folder not found yet. Retrying in {}ms...", rptPollIntervalMillis);
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            continue;
        }

        try (Stream<Path> flowstepDirs = Files.list(docgenRoot).filter(Files::isDirectory)) {
            Optional<Path> flowstepPathOpt = flowstepDirs.findFirst();

            if (flowstepPathOpt.isPresent()) {
                Path outputPath = flowstepPathOpt.get().resolve("output");
                if (Files.exists(outputPath)) {
                    try (Stream<Path> files = Files.list(outputPath)) {
                        Optional<File> xmlFile = files
                            .filter(Files::isRegularFile)
                            .map(Path::toFile)
                            .filter(f -> f.getName().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                            .findFirst();

                        if (xmlFile.isPresent()) {
                            logger.info("✅ Found _STDDELIVERYFILE.xml at: {}", xmlFile.get().getAbsolutePath());
                            return xmlFile.get();
                        } else {
                            logger.info("Output folder exists but _STDDELIVERYFILE.xml not yet created. Retrying...");
                        }
                    }
                } else {
                    logger.info("Flowstep output folder not found yet. Retrying...");
                }
            } else {
                logger.info("No flowstep folder found under docgen yet. Retrying...");
            }
        } catch (Exception e) {
            logger.error("Error while searching for XML file", e);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    logger.error("❌ Timed out after {} seconds waiting for _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
    return null;
}

private String callOrchestrationBatchApi(String token, KafkaMessage msg) {
    try {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
        ResponseEntity<Map> response = restTemplate.exchange(otOrchestrationApiUrl, HttpMethod.POST, request, Map.class);

        // ✅ Log full response body from OT
        logger.info("📨 OT Orchestration Response: {}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
        if (data != null && !data.isEmpty()) {
            return (String) data.get(0).get("jobId");
        }
    } catch (Exception e) {
        logger.error("❌ Failed OT Orchestration call", e);
    }
    return null;
}
