# You can tune this value based on environment
kafka.listener.concurrency=3

package com.nedbank.kafka.filemanage.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaConsumerConfig sets up a secure Kafka consumer configuration for the application.
 * This configuration uses SSL for encrypted communication with the Kafka cluster and
 * enables configurable concurrency for parallel batch processing.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String consumerGroupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.enable.auto.commit}")
    private String enableAutoCommit;

    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializer;

    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializer;

    @Value("${kafka.consumer.security.protocol}")
    private String securityProtocol;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    // ✅ NEW: Configurable concurrency level (default is 1 if not provided)
    @Value("${kafka.listener.concurrency:1}")
    private Integer listenerConcurrency;

    /**
     * Configures the Kafka listener container factory.
     * - Enables manual acknowledgment
     * - Sets up SSL-based security
     * - Allows configurable concurrency
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        // SSL Security Configuration
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        props.put("ssl.endpoint.identification.algorithm", ""); // Disable hostname verification

        // Offset settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // ✅ Use configurable concurrency
        factory.setConcurrency(listenerConcurrency);

        return factory;
    }
}

We’ve made the Kafka listener concurrency configurable using kafka.listener.concurrency (default is 1). This will help handle multiple small batches in parallel when needed.

No changes were made to the existing logic — only the concurrency setup is now flexible.
