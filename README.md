private void initSecrets() {
    if (accountKey != null && accountName != null && containerName != null) {
        return;
    }

    try {
        logger.info("🔐 Fetching secrets from Azure Key Vault...");
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        // ✅ Updated secret names from your provided URLs
        accountKey = getSecret(secretClient, "ecm-fm-account-key");
        accountName = getSecret(secretClient, "ecm-fm-account-name");
        containerName = getSecret(secretClient, "ecm-fm-container-name");

        if (accountKey == null || accountKey.isBlank() ||
                accountName == null || accountName.isBlank() ||
                containerName == null || containerName.isBlank()) {
            throw new CustomAppException("One or more secrets are null/empty from Key Vault", 400, HttpStatus.BAD_REQUEST);
        }

        logger.info("✅ Secrets fetched successfully from Azure Key Vault.");
    } catch (Exception e) {
        logger.error("❌ Failed to initialize secrets from Key Vault: {}", e.getMessage(), e);
        throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
