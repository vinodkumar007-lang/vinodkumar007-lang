package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for setting up Kafka producer with SSL security.
 * This configuration enables Kafka message publishing with secure connectivity using keystore and truststore credentials.
 */
@Configuration
public class KafkaProducerConfig {

    // Kafka broker address (e.g., host:port)
    @Value("${kafka.producer.bootstrap.servers}")
    private String bootstrapServers;

    // Security protocol (e.g., SSL)
    @Value("${kafka.producer.security.protocol}")
    private String securityProtocol;

    // Truststore configuration
    @Value("${kafka.producer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.producer.ssl.truststore.password}")
    private String truststorePassword;

    // Keystore configuration
    @Value("${kafka.producer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.producer.ssl.keystore.password}")
    private String keystorePassword;

    // Key password for producer's private key
    @Value("${kafka.producer.ssl.key.password}")
    private String keyPassword;

    // SSL Protocol (e.g., TLSv1.2)
    @Value("${kafka.producer.ssl.protocol}")
    private String sslProtocol;

    /**
     * Configures and provides a Kafka ProducerFactory with SSL settings.
     *
     * @return ProducerFactory for producing Kafka messages.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Basic producer settings
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // SSL Security configurations
        configProps.put("security.protocol", securityProtocol);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        configProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);

        // Disable hostname verification (use with caution in production)
        configProps.put("ssl.endpoint.identification.algorithm", "");

        // Recommended tuning options for reliability and timeout handling
        configProps.put(ProducerConfig.RETRIES_CONFIG, 5);                          // Retry up to 5 times
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);          // Request timeout
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);               // Max block while creating producer
        configProps.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 30000);            // Metadata refresh interval
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);             // Backoff between retries

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Provides a KafkaTemplate for sending messages to Kafka.
     *
     * @return KafkaTemplate instance wired to the configured producer factory.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}

spring.kafka.producer.bootstrap-servers=nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# ===== SSL Settings =====
spring.kafka.producer.security.protocol=SSL
spring.kafka.producer.ssl.truststore-location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
spring.kafka.producer.ssl.truststore-password=nedbank1
spring.kafka.producer.ssl.keystore-location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
spring.kafka.producer.ssl.keystore-password=3dX7y3Yz9Jv6L4F
spring.kafka.producer.ssl.key-password=3dX7y3Yz9Jv6L4F
