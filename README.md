EnvironmentCredential returns a token
2025-09-22T16:14:03.284+02:00  INFO 1 --- [t.azure.net/...] c.a.s.k.secrets.SecretAsyncClient        : Retrieved secret - otds-token-dev
2025-09-22T16:14:03.285+02:00  INFO 1 --- [pool-1-thread-4] c.n.k.f.service.KafkaListenerService     : 🚀 [batchId: 6f78b601-2091-4ef3-bbc0-3af3fe3480f6] Calling Orchestration API: ${OT_SERVICE_NGB_URL}
2025-09-22T16:14:03.285+02:00  INFO 1 --- [pool-1-thread-4] c.n.k.f.service.KafkaListenerService     : 📡 Initiating OT orchestration call to URL: ${OT_SERVICE_NGB_URL} for batchId: 6f78b601-2091-4ef3-bbc0-3af3fe3480f6 and sourceSystem: NGB
2025-09-22T16:14:03.286+02:00 ERROR 1 --- [pool-1-thread-4] c.n.k.f.service.KafkaListenerService     : ❌ Exception during OT orchestration call for batchId: 6f78b601-2091-4ef3-bbc0-3af3fe3480f6 - Not enough variable values available to expand 'OT_SERVICE_NGB_URL'

package com.nedbank.kafka.filemanage.service;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "source")
public class SourceSystemProperties {

    private List<SystemConfig> systems;

    public Optional<SystemConfig> getConfigForSourceSystem(String name) {
        return systems.stream()
                .filter(s -> s.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    @Getter
    @Setter
    public static class SystemConfig {
        private String name;
        private String url;
        private String token;
    }
}
