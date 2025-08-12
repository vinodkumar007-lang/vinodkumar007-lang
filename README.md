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
