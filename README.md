package com.nedbank.kafka.filemanage.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
@ConfigurationProperties(prefix = "source")
@Data
public class SourceSystemProperties {

    private List<SystemConfig> systems;

    @Data
    public static class SystemConfig {
        private String name;      // e.g. NEDTRUST
        private String url;       // e.g. ${OT_SERVICE_CADNT1Service_URL}
        private String token;     // e.g. otds-token-dev
        private String jobName;   // optional - only for systems with multiple job names
    }

    /**
     * Find configuration for given source system and job name.
     * Falls back to generic (no jobName) config if specific not found.
     */
    public Optional<SystemConfig> getConfigForSourceSystem(String sourceSystem, String jobName) {

        // 1️⃣ Try to find exact match (sourceSystem + jobName)
        Optional<SystemConfig> match = systems.stream()
                .filter(s -> s.getName().equalsIgnoreCase(sourceSystem)
                        && s.getJobName() != null
                        && s.getJobName().equalsIgnoreCase(jobName))
                .findFirst();

        // 2️⃣ If not found, try generic (no jobName) match
        if (match.isEmpty()) {
            match = systems.stream()
                    .filter(s -> s.getName().equalsIgnoreCase(sourceSystem)
                            && s.getJobName() == null)
                    .findFirst();
        }

        // 3️⃣ Return final result
        return match;
    }
}

# Generic system (no jobName)
source.systems[0].name=DEBTMAN
source.systems[0].url=${OT_SERVICE_DEBTMAN_URL}
source.systems[0].token=otds-token-dev

# Job-specific entries
source.systems[1].name=NEDTRUST
source.systems[1].jobName=CADNT1
source.systems[1].url=${OT_SERVICE_CADNT1Service_URL}
source.systems[1].token=otds-token-dev

source.systems[2].name=NEDTRUST
source.systems[2].jobName=CADNT9
source.systems[2].url=${OT_SERVICE_CADNT9Service_URL}
source.systems[2].token=otds-token-dev

String sourceSystem = kafkaMessage.getSourceSystem();
String jobName = kafkaMessage.getJobName();

Optional<SourceSystemProperties.SystemConfig> configOpt =
        sourceSystemProperties.getConfigForSourceSystem(sourceSystem, jobName);

if (configOpt.isPresent()) {
    SourceSystemProperties.SystemConfig config = configOpt.get();
    log.info("Using URL: {} and token: {} for sourceSystem: {} and jobName: {}",
             config.getUrl(), config.getToken(), sourceSystem, jobName);
} else {
    log.warn("No configuration found for sourceSystem={} and jobName={}", sourceSystem, jobName);
}
