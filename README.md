/**
 * Retrieves the OTDS token from Azure Key Vault.
 *
 * @return OTDS token string
 */
public String getOtdsToken() {
    try {
        logger.info("🔐 Fetching OTDS token from Azure Key Vault...");

        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        String token = getSecret(secretClient, "otds_token");

        if (token == null || token.trim().isEmpty()) {
            throw new CustomAppException("❌ OTDS token is empty or missing in Key Vault", 
                    400, HttpStatus.BAD_REQUEST);
        }

        logger.info("✅ OTDS token fetched successfully.");
        return token;
    } catch (Exception e) {
        logger.error("❌ Failed to fetch OTDS token: {}", e.getMessage(), e);
        throw new CustomAppException("ERR_FETCH_OTDS_TOKEN", 
                500, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}

String otdsToken = blobStorageService.getOtdsToken();
callOractstatonurl(otdsToken, otherParams...);
