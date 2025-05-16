2025-05-16T10:01:21.773+02:00 ERROR 20964 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Failed to parse corrected JSON: Unexpected character ('\' (code 92)): was expecting comma to separate Object entries
 at [Source: (String)"{"sourceSystem":"DEBTMAN"\, "timestamp":"2025"-05-12T09:"06":"07".843956600+02:"00""Africa/Johannesburg"\, "batchFiles":"BatchFile("objectId":"1037A096-0000-CE1A-A484-3290CA7938C2"\, "repositoryId":"BATCH"\, "fileLocation":/path/to/file1\, "validationStatus":"valid")"\, "consumerReference":"12345"\, "processReference":"check_process_reference"\, "batchControlFileData":"null"}"; line: 1, column: 27]

com.fasterxml.jackson.core.JsonParseException: Unexpected character ('\' (code 92)): was expecting comma to separate Object entries
 at [Source: (String)"{"sourceSystem":"DEBTMAN"\, "timestamp":"2025"-05-12T09:"06":"07".843956600+02:"00""Africa/Johannesburg"\, "batchFiles":"BatchFile("objectId":"1037A096-0000-CE1A-A484-3290CA7938C2"\, "repositoryId":"BATCH"\, "fileLocation":/path/to/file1\, "validationStatus":"valid")"\, "consumerReference":"12345"\, "processReference":"check_process_reference"\, "batchControlFileData":"null"}"; line: 1, column: 27]
	at com.fasterxml.jackson.core.JsonParser._constructError(JsonParser.java:2418) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportError(ParserMinimalBase.java:749) ~[jackson-core-2.14.1.jar:2.14.1]
	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportUnexpectedChar(ParserMinimalBase.java:673) ~[jackson-core-2.14.1.jar:2.14.1]
	at
