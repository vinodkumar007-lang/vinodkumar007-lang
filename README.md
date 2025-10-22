2025-10-22T15:49:18.655+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Failed to serialize audit message for batchId 57c8aaa1-105d-41a0-8864-faeaaed89ba7: Java 8 date/time type `java.time.Instant` not supported by default: add Module "com.fasterxml.jackson.datatype:jackson-datatype-jsr310" to enable handling (through reference chain: com.nedbank.kafka.filemanage.model.ECPBatchAudit["startTime"])

com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Java 8 date/time type `java.time.Instant` not supported by default: add Module "com.fasterxml.jackson.datatype:jackson-datatype-jsr310" to enable handling (through reference chain: com.nedbank.kafka.filemanage.model.ECPBatchAudit["startTime"])
	at com.fasterxml.jackson.databind.exc.InvalidDefinitionException.from(InvalidDefinitionException.java:77) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.SerializerProvider.reportBadDefinition(SerializerProvider.java:1306) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.ser.impl.UnsupportedTypeSerializer.serialize(UnsupportedTypeSerializer.java:35) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.ser.BeanPropertyWriter.serializeAsField(BeanPropertyWriter.java:733) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.ser.std.BeanSerializerBase.serializeFields(BeanSerializerBase.java:774) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.ser.BeanSerializer.serialize(BeanSerializer.java:178) ~[jackson-databind-2.14.1.jar!/:2.14.1]
	at com.fasterxml.jackson.databind.ser.DefaultSerializerProvider._serialize(DefaultSerializerProvider.java:480) ~[jackson-databind-2.14.1.jar!/:2.14.1]
