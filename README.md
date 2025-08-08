try {
    logger.info("🔐 Fetching secrets from Azure Key Vault...");
    logger.info("📌 Key Vault URL          : {}", keyVaultUrl);
    logger.info("📌 Secret Names Requested : {}, {}, {}",
            fmAccountKey, fmAccountName, fmContainerName);

    SecretClient secretClient = new SecretClientBuilder()
            .vaultUrl(keyVaultUrl)
            .credential(new DefaultAzureCredentialBuilder().build())
            .buildClient();

    accountKey = getSecret(secretClient, fmAccountKey);
    accountName = getSecret(secretClient, fmAccountName);
    containerName = getSecret(secretClient, fmContainerName);

    logger.info("📦 Azure Storage Secrets fetched:");
    logger.info("   • Account Key    : {}", accountKey);
    logger.info("   • Account Name   : {}", accountName);
    logger.info("   • Container Name : {}", containerName);

    if (accountKey == null || accountName == null || containerName == null) {
        throw new CustomAppException(BlobStorageConstants.ERR_MISSING_SECRETS, 400, HttpStatus.BAD_REQUEST);
    }

    lastSecretRefreshTime = Instant.now();
    logger.info("✅ Secrets fetched successfully from Key Vault.");
} catch (Exception e) {
    logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
    throw new CustomAppException(BlobStorageConstants.ERR_KV_FAILURE, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
}
