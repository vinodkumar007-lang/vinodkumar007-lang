# Allow long processing time (e.g., OT takes up to 1 hour)
spring.kafka.consumer.properties.max.poll.interval.ms=4200000

# Ensure only one message is polled at a time
spring.kafka.consumer.max-poll-records=1
