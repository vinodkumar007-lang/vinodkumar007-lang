public String getOtdsToken() {
    try {
        logger.info("🔐 Fetching OTDS token name from config...");

        // Get the first system's token name from config
        String secretName = sourceSystemsConfig.getSystems().stream()
                .findFirst()
                .map(SourceSystemsConfig.SystemConfig::getToken)
                .orElseThrow(() -> new CustomAppException(
                        "❌ No OTDS token configured in source.systems",
                        400, HttpStatus.BAD_REQUEST));

        logger.info("🔑 OTDS token secret name from config: {}", secretName);

        // Fetch the actual token value from Key Vault
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        String tokenValue = getSecret(secretClient, secretName);

        if (tokenValue == null || tokenValue.isBlank()) {
            throw new CustomAppException("❌ OTDS token value is empty or missing in Key Vault",
                    400, HttpStatus.BAD_REQUEST);
        }

        logger.info("✅ OTDS token fetched successfully from Key Vault.");
        return tokenValue;

    } catch (Exception e) {
        logger.error("❌ Failed to fetch OTDS token: {}", e.getMessage(), e);
        throw new CustomAppException("ERR_FETCH_OTDS_TOKEN", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}
