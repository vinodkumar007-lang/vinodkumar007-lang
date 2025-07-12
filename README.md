025-07-12T12:33:44.239+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='null' and payload='{"message":"Success","status":"success","summaryPayload":{"summaryPayload":{"batchID":"6e8e56f7-a4fe...' to topic str-ecp-batch-composition-complete:

org.apache.kafka.common.errors.TimeoutException: Topic str-ecp-batch-composition-complete not present in metadata after 60000 ms.

2025-07-12T12:33:44.240+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(t
