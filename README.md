private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            if (lastSecretRefreshTime != null &&
                    Instant.now().toEpochMilli() - lastSecretRefreshTime.toEpochMilli() < BlobStorageConstants.SECRET_CACHE_TTL_MS) {
                return;
            }
        }

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            logger.info("📌 Key Vault URL          : {}", keyVaultUrl);
            logger.info("📌 Secret Names Requested : {}, {}, {}",
                    fmAccountKey, fmAccountName, fmContainerName);

            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = fetchSecret(secretClient, fmAccountKey);
            accountName = fetchSecret(secretClient, fmAccountName);
            containerName = fetchSecret(secretClient, fmContainerName);

            logger.info("📦 Azure Storage Secrets fetched:");

            if (accountKey == null || accountName == null || containerName == null) {
                throw new CustomAppException(BlobStorageConstants.ERR_MISSING_SECRETS, 400, HttpStatus.BAD_REQUEST);
            }

            lastSecretRefreshTime = Instant.now();
            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException(BlobStorageConstants.ERR_KV_FAILURE, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
