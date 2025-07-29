Input variables like sourceSystem and consumerRef are now sanitized to handle null, empty, or unsafe characters. Fallback values are also used to ensure blob path consistency and avoid runtime issues
