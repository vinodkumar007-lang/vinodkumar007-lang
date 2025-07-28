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
 * ensures manual acknowledgment with single-threaded message consumption.
 */
@Configuration
public class KafkaConsumerConfig {

    // Kafka bootstrap server address (comma-separated if multiple)
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    // Kafka consumer group ID
    @Value("${kafka.consumer.group.id}")
    private String consumerGroupId;

    // Determines behavior when no offset is found ("earliest", "latest", etc.)
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    // Whether Kafka should auto-commit offsets or not (typically "false")
    @Value("${kafka.consumer.enable.auto.commit}")
    private String enableAutoCommit;

    // Fully qualified class name of key deserializer (e.g., StringDeserializer)
    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializer;

    // Fully qualified class name of value deserializer
    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializer;

    // Security protocol (e.g., "SSL")
    @Value("${kafka.consumer.security.protocol}")
    private String securityProtocol;

    // SSL truststore path
    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    // SSL truststore password
    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    // SSL keystore path
    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    // SSL keystore password
    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    // SSL private key password
    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    // SSL protocol (e.g., TLSv1.2)
    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    /**
     * Configures the Kafka listener container factory.
     * - Enables manual acknowledgment
     * - Sets up SSL-based security
     * - Creates a single-threaded Kafka consumer (concurrency = 1)
     *
     * @return ConcurrentKafkaListenerContainerFactory configured with SSL and manual ack mode
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        // Define Kafka consumer configuration properties
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        // Security and SSL configuration
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        props.put("ssl.endpoint.identification.algorithm", ""); // Disable hostname verification

        // Offset and commit settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        // Create the Kafka consumer factory and listener container
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));

        // Use manual acknowledgment mode (commits only when explicitly acknowledged)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Set to single-threaded consumption (one message at a time)
        factory.setConcurrency(1);

        return factory;
    }
}
