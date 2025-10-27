 Optional<SourceSystemProperties.SystemConfig> configOpt =
                    sourceSystemProperties.getConfigForSourceSystem(
                            message.getSourceSystem(),
                            message.getJobName()
                    );

            String url;
            String token;
            if (configOpt.isPresent()) {
                SourceSystemProperties.SystemConfig config = configOpt.get();
                url = config.getUrl();
                token = config.getToken(); // Always from index 0
                logger.info("Using URL={} for {}:{} with token={}",
                        url, message.getSourceSystem(),
                        message.getJobName(), token);
            } else {
                token = "";
                url = null;
                logger.warn("No config found for sourceSystem={} and jobName={}",
                        message.getSourceSystem(), message.getJobName());
            }

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sanitizedSourceSystem);
                return;
            }


			// Dynamic lookup for orchestration
            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(sanitizedSourceSystem);

            if (matchingConfig.isEmpty()) {
                logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sanitizedSourceSystem);
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String secretName = sourceSystemProperties.getSystems().get(0).getToken();
            String token = blobStorageService.getSecret(secretName);
