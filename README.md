2025-05-16T09:54:30.636+02:00  WARN 22376 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Failed to parse JSON, attempting to convert POJO-like message: PublishEvent(sourceSystem=DEBTMAN, timestamp=2025-05-12T09:06:07.843956600+02:00[Africa/Johannesburg], batchFiles=[BatchFile(objectId={1037A096-0000-CE1A-A484-3290CA7938C2}, repositoryId=BATCH, fileLocation=/path/to/file1, validationStatus=valid)], consumerReference=12345, processReference=check_process_reference, batchControlFileData=null)
2025-05-16T09:54:30.647+02:00 ERROR 22376 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Failed to parse corrected JSON: Unrecognized token 'DEBTMAN': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
 at [Source: (String)"{"sourceSystem":DEBTMAN, "timestamp":2025-05-12T09:06:07.843956600+02:00[Africa/Johannesburg], batchFiles=[{"objectId":{1037A096-0000-CE1A-A484-3290CA7938C2}, "repositoryId":BATCH, "fileLocation":/path/to/file1, "validationStatus":valid}], "consumerReference":12345, "processReference":check_process_reference, "batchControlFileData":null}"; line: 1, column: 24]

com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'DEBTMAN': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
 at [Source: (String)"{"sourceSystem":DEBTMAN, "timestamp":2025-05-12T09:06:07.843956600+02:00[Africa/Johannesburg], batchFiles=[{"objectId":{1037A096-0000-CE1A-A484-3290CA7938C2}, "repositoryId":BATCH, "fileLocation":/path/to/file1, "validationStatus":valid}], "consumerReference":12345, "processReference":check_process_reference, "batchControlFileData":null}"; line: 1, column: 24]
	at com.fasterxml.jackson.core.JsonParser._constructError(JsonParser.java:2418) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportError(ParserMinimalBase.java:759) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser._reportInvalidToken(ReaderBasedJsonParser.java:3038) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser._handleOddValue(ReaderBasedJsonParser.java:2079) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.json.ReaderBasedJsonParser.nextFieldName(ReaderBasedJsonParser.java:1026) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.deser.std.BaseNodeDeserializer._deserializeContainerNoRecursion(JsonNodeDeserializer.java:534) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer.deserialize(JsonNodeDeserializer.java:98) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer.deserialize(JsonNodeDeserializer.java:23) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.deser.DefaultDeserializationContext.readRootValue(DefaultDeserializationContext.java:323) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.ObjectMapper._readTreeAndClose(ObjectMapper.java:4772) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.databind.ObjectMapper.readTree(ObjectMapper.java:3124) ~[jackson-databind-2.14.1.jar:2.14.1]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.handleMessage(KafkaListenerService.java:75) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:54) ~[classes/:na]
	at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:32) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:207) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:152) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndH
