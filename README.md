spring.kafka.producer.properties.request.timeout.ms=60000
spring.kafka.producer.properties.max.block.ms=180000
spring.kafka.producer.properties.metadata.max.age.ms=30000
spring.kafka.producer.properties.retries=5
spring.kafka.producer.properties.retry.backoff.ms=5000

kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response)).get(60, TimeUnit.SECONDS);
