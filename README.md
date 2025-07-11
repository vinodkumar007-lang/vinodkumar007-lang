2025-07-11T14:00:30.788+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🔎 Searching for _STDDELIVERYFILE.xml in /mnt/nfs/dev-exstream/dev-SA/jobs/6b5caa2e-eb44-4ecc-a10c-e53b5778d2da/70796c59-75ff-4085-894a-eeb13bece71d/docgen
2025-07-11T14:00:30.806+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:00:35.807+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:00:40.807+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:00:45.808+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:00:50.808+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:00:55.809+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:00.810+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:05.810+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:10.811+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:15.811+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:20.812+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:25.812+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:30.813+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T14:01:35.813+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms

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
