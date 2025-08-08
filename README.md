try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

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
