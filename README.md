java.lang.NumberFormatException: For input string: "1.748351245695411E9"
	at java.base/java.lang.NumberFormatException.forInputString(NumberFormatException.java:67) ~[na:na]
	at java.base/java.lang.Long.parseLong(Long.java:711) ~[na:na]
	at java.base/java.lang.Long.parseLong(Long.java:836) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.instantToIsoString(KafkaListenerService.java:276) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:116) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.listen(KafkaListenerService.java:83) ~[classes/:na]
	at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:30) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.

 private String instantToIsoString(Double epochMillis) {
        return Instant.ofEpochMilli(Long.parseLong(String.valueOf(epochMillis))).toString();
    }
