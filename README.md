private String instantToIsoString(Double epochMillis) {
        // Handle scientific notation and decimals safely:
        long millis;
        try {
            millis = doubleToLongEpochMillis(epochMillis);
        } catch (Exception e) {
            logger.warn("Invalid timestamp value: {}. Defaulting to epoch=0", epochMillis);
            millis = 0L;
        }
        return Instant.ofEpochMilli(millis).toString();
    }

    private String instantToDateString(Double epochMillis) {
        // Extract date in YYYYMMDD format safely from epoch millis
        long millis;
        try {
            millis = doubleToLongEpochMillis(epochMillis);
        } catch (Exception e) {
            logger.warn("Invalid timestamp value: {}. Defaulting to epoch=0", epochMillis);
            millis = 0L;
        }
        return Instant.ofEpochMilli(millis).toString().substring(0, 10).replace("-", "");
    }

    private long doubleToLongEpochMillis(Double epochMillis) {
        // Convert Double epoch millis safely:
        // Handles scientific notation and decimals by flooring the value
        if (epochMillis == null) {
            throw new IllegalArgumentException("epochMillis is null");
        }
        return (long) Math.floor(epochMillis);
    }
