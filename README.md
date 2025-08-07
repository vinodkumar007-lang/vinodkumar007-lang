2025-08-07T08:34:36.524+02:00  WARN 1 --- [           main] ConfigServletWebServerApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'sourceSystemProperties' defined in URL [jar:file:/deployments/file-manager-1.0-SNAPSHOT.jar!/BOOT-INF/classes!/com/nedbank/kafka/filemanage/service/SourceSystemProperties.class]: Cannot bind @ConfigurationProperties for bean 'sourceSystemProperties'. Ensure that @ConstructorBinding has not been applied to regular bean
2025-08-07T08:34:36.623+02:00  INFO 1 --- [           main] o.apache.catalina.core.StandardService   : Stopping service [Tomcat]
2025-08-07T08:34:36.924+02:00  INFO 1 --- [           main] .s.b.a.l.ConditionEvaluationReportLogger : 
Error starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled.
2025-08-07T08:34:37.422+02:00 ERROR 1 --- [           main] o.s.boot.SpringApplication               : Application run failed
org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'sourceSystemProperties' defined in URL [jar:file:/deployments/file-manager-1.0-SNAPSHOT.jar!/BOOT-INF/classes!/com/nedbank/kafka/filemanage/service/SourceSystemProperties.class]: Cannot bind @ConfigurationProperties for bean 'sourceSystemProperties'. Ensure that @ConstructorBinding has not been applied to regular bean
 at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:606) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:521) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:326) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:324) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:961) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:915) ~[spring-context-6.0.2.jar!/:6.0.2]
 at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:584) ~[spring-context-6.0.2.jar!/:6.0.2]
 at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:146) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:730) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:432) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:308) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:1302) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.SpringApplication.run(SpringApplication.java:1291) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at com.nedbank.kafka.filemanage.Application.main(Application.java:9) ~[classes!/:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
 at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
 at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
 at org.springframework.boot.loader.MainMethodRunner.run(MainMethodRunner.java:49) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.Launcher.launch(Launcher.java:95) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.Launcher.launch(Launcher.java:58) ~[file-manager-1.0-SNAPSHOT.jar:na]
 at org.springframework.boot.loader.JarLauncher.main(JarLauncher.java:65) ~[file-manager-1.0-SNAPSHOT.jar:na]
Caused by: java.lang.IllegalStateException: Cannot bind @ConfigurationProperties for bean 'sourceSystemProperties'. Ensure that @ConstructorBinding has not been applied to regular bean
 at org.springframework.util.Assert.state(Assert.java:76) ~[spring-core-6.0.2.jar!/:6.0.2]
 at org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor.bind(ConfigurationPropertiesBindingPostProcessor.java:86) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.boot.context.properties.ConfigurationPropertiesBindingPostProcessor.postProcessBeforeInitialization(ConfigurationPropertiesBindingPostProcessor.java:78) ~[spring-boot-3.0.0.jar!/:3.0.0]
 at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.applyBeanPostProcessorsBeforeInitialization(AbstractAutowireCapableBeanFactory.java:420) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.initializeBean(AbstractAutowireCapableBeanFactory.java:1743) ~[spring-beans-6.0.2.jar!/:6.0.2]
 at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:599) ~[spring-beans-6.0.2.jar!/:6.0.2]
 ... 23 common frames omitted


     @Autowired
    private SourceSystemProperties sourceSystemProperties;
    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate, SourceSystemProperties sourceSystemProperties) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }
    /**
     * Kafka consumer method to handle messages from input topic.
     * Performs validation on message structure, downloads files,
     * and triggers orchestration API.
     *
     * @param rawMessage Raw Kafka message in JSON string format
     * @param ack        Kafka acknowledgment to commit offset manually
     */
    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        String batchId = "";
        try {
            logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            batchId = message.getBatchId();
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
                ack.acknowledge();
                return;
            }

            long dataCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                    .count();

            if (dataCount == 1 && refCount == 0) {
                logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
            } else if (dataCount > 1) {
                logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 0 && refCount > 0) {
                logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 1 && refCount > 0) {
                logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
                message.setBatchFiles(batchFiles);
            } else {
                logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
                ack.acknowledge();
                return;
            }

            String sanitizedBatchId = batchId.replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);
            String sanitizedSourceSystem = message.getSourceSystem().replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);

            Path batchDir = Paths.get(mountPath, INPUT_FOLDER, sanitizedSourceSystem, sanitizedBatchId);
            if (Files.exists(batchDir)) {
                logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
                try (Stream<Path> files = Files.walk(batchDir)) {
                    files.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                    logger.info("🧹 [batchId: {}] Cleaned existing input directory: {}", batchId, batchDir);
                } catch (IOException e) {
                    logger.error("❌ [batchId: {}] Failed to clean directory {} - {}", batchId, batchDir, e.getMessage(), e);
                    throw e;
                }
            }

            Files.createDirectories(batchDir);
            logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFilename());

                try {
                    if (Files.exists(localPath)) {
                        logger.warn("♻️ [batchId: {}] File already exists, overwriting: {}", batchId, localPath);
                        Files.delete(localPath);
                    }

                    blobStorageService.downloadFileToLocal(blobUrl, localPath);

                    if (!Files.exists(localPath)) {
                        logger.error("❌ [batchId: {}] File missing after download: {}", batchId, localPath);
                        throw new IOException("Download failed for: " + localPath);
                    }

                    file.setBlobUrl(localPath.toString());
                    logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
                } catch (Exception e) {
                    logger.error("❌ [batchId: {}] Failed to download or overwrite file: {} - {}", batchId, blobUrl, e.getMessage(), e);
                    throw e;
                }
            }

            // 🔁 New logic starts here — dynamic lookup
            String sourceSystem = message.getSourceSystem();
            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(sourceSystem);

            if (matchingConfig.isEmpty()) {
                logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String token = config.getToken();

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            // ✅ Acknowledge before async OT call
            ack.acknowledge();

            String finalBatchId = batchId;
            executor.submit(() -> {
                try {
                    logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                    OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                    logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                    processAfterOT(message, otResponse);
                } catch (Exception ex) {
                    logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
                }
            });

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }
