package com.nedbank.kafka.filemanage.config;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Audit Kafka Producer Configuration with SSL settings.
 * Used to send audit events securely to Kafka.
 */
@Configuration
public class KafkaAuditProducerConfig {

    @Value("${audit.kafka.producer.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${audit.kafka.producer.security.protocol}")
    private String securityProtocol;

    @Value("${audit.kafka.producer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${audit.kafka.producer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${audit.kafka.producer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${audit.kafka.producer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${audit.kafka.producer.ssl.key.password}")
    private String keyPassword;

    @Value("${audit.kafka.producer.ssl.protocol}")
    private String sslProtocol;

    @PostConstruct
    public void logAuditKafkaProps() {
        System.out.println("========== AUDIT KAFKA PRODUCER CONFIG ==========");
        System.out.println("Bootstrap Servers : " + bootstrapServers);
        System.out.println("Security Protocol : " + securityProtocol);
        System.out.println("Truststore Location : " + truststoreLocation);
        System.out.println("truststore Password   : " + truststorePassword);
        System.out.println("keystore Location : " + keystoreLocation);
        System.out.println("Keystore Password   : " + keystorePassword);
        System.out.println("keyPassword   : " + keyPassword);
        System.out.println("sslProtocol             : " + sslProtocol);
        System.out.println("=================================================");
    }

    @Bean(name = "auditProducerFactory")
    public ProducerFactory<String, String> auditProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // SSL Security configs
        configProps.put("security.protocol", securityProtocol);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        configProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);

        // Reliability settings
        configProps.put(ProducerConfig.RETRIES_CONFIG, 5);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);
        configProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "auditKafkaTemplate")
    public KafkaTemplate<String, String> auditKafkaTemplate() {
        return new KafkaTemplate<>(auditProducerFactory());
    }
}
