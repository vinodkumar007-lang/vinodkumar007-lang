This property is a problem for production , can we fix the endpoint verification?

// Recommended tuning options for reliability and timeout handling
        configProps.put(ProducerConfig.RETRIES_CONFIG, 5);                          // Retry up to 5 times
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);          // Request timeout
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);               // Max block while creating producer
        configProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);            // Metadata refresh interval
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);             // Backoff between retries
