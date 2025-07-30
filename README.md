/**
 * Lazily initializes and caches secrets required for Azure Blob operations.
 * 
 * <p>This method ensures that secrets such as {@code accountName}, {@code accountKey}, and 
 * {@code containerName} are fetched from Azure Key Vault only when needed.</p>
 *
 * <p>To avoid repeated Key Vault calls, the secrets are cached and refreshed based on a 
 * configurable TTL (time-to-live). If the TTL has not expired since the last refresh, the 
 * cached values are reused.</p>
 *
 * <p>This design balances performance with sensitivity to secret updates in Key Vault.</p>
 */
