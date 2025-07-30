I assume this is to ensure that the value are cached? If so, how do changes in the values get propagated to the container? Should you not use a cache to expire the values after a while?

private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) {
            if (lastSecretRefreshTime != null &&
                    Instant.now().toEpochMilli() - lastSecretRefreshTime.toEpochMilli() < SECRET_CACHE_TTL_MS) {
                return;
            }
        }
