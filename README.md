package com.nedbank.kafka.filemanage.service;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "source")
@Data
public class SourceSystemProperties {

    /**
     * List of source system configurations.
     * Each entry can have a jobName (optional).
     * The token is shared and always taken from index 0.
     */
    private List<SystemConfig> systems;

    @Data
    public static class SystemConfig {
        private String name;      // e.g., NEDTRUST, DEBTMAN
        private String url;       // Orchestration URL for this source system/job
        private String token;     // Shared token (taken from index 0)
        private String jobName;   // Optional jobName (for multiple jobs per source system)
    }

    /**
     * Get configuration for a given source system and jobName.
     * If job-specific config exists, returns that.
     * Otherwise falls back to generic (jobName=null) config.
     * Always attaches shared token from systems[0].
     *
     * @param sourceSystem the source system name from Kafka message
     * @param jobName      the job name from Kafka message (optional)
     * @return Optional<SystemConfig>
     */
    public Optional<SystemConfig> getConfigForSourceSystem(String sourceSystem, String jobName) {
        if (systems == null || systems.isEmpty()) {
            return Optional.empty();
        }

        // Always take token from first entry
        String sharedToken = systems.get(0).getToken();

        String src = sourceSystem != null ? sourceSystem.trim() : "";
        String job = jobName != null ? jobName.trim() : "";

        // 1️⃣ Exact match: sourceSystem + jobName
        Optional<SystemConfig> match = systems.stream()
                .filter(s -> s.getName().equalsIgnoreCase(src)
                        && s.getJobName() != null
                        && s.getJobName().equalsIgnoreCase(job))
                .findFirst();

        // 2️⃣ Fallback: sourceSystem only (jobName=null or blank)
        if (match.isEmpty()) {
            match = systems.stream()
                    .filter(s -> s.getName().equalsIgnoreCase(src)
                            && (s.getJobName() == null || s.getJobName().isBlank()))
                    .findFirst();
        }

        // 3️⃣ Attach shared token from index 0
        match.ifPresent(s -> s.setToken(sharedToken));

        return match;
    }
}

Optional<SourceSystemProperties.SystemConfig> configOpt =
        sourceSystemProperties.getConfigForSourceSystem(
            kafkaMessage.getSourceSystem(),
            kafkaMessage.getJobName()
        );

if (configOpt.isPresent()) {
    SourceSystemProperties.SystemConfig config = configOpt.get();
    String url = config.getUrl();
    String token = config.getToken(); // Always from index 0
    log.info("Using URL={} for {}:{} with token={}",
             url, kafkaMessage.getSourceSystem(),
             kafkaMessage.getJobName(), token);
} else {
    log.warn("No config found for sourceSystem={} and jobName={}",
             kafkaMessage.getSourceSystem(), kafkaMessage.getJobName());
}
