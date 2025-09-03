/**
 * Internal helper that fetches a secret using an existing SecretClient.
 * Used internally for efficiency when a SecretClient is already available.
 */
private String fetchSecret(SecretClient client, String secretName) {
    try {
        return client.getSecret(secretName).getValue();
    } catch (Exception e) {
        logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
        throw new CustomAppException(BlobStorageConstants.ERR_FETCH_SECRET + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}

/**
 * Public convenience method that fetches a secret by name.
 * Builds its own SecretClient internally.
 *
 * Example: String token = blobStorageService.getSecret("otds-secret-name");
 */
public String getSecret(String secretName) {
    try {
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        return fetchSecret(secretClient, secretName);
    } catch (Exception e) {
        logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
        throw new CustomAppException(BlobStorageConstants.ERR_FETCH_SECRET + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
    }
}

======
String secretName = sourceSystemProperties.getSystems().get(0).getToken();
String token = blobStorageService.getSecret(secretName);
