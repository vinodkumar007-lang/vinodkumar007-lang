2025-07-12T11:17:08.370+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/298142ba-4544-45d8-8e49-bee742fd2f0b/7af5284e-95e4-4766-9217-f0b0cefb7405/docgen/a8a7c9e6-2207-4bff-952e-c6e5d78d8eeb/output/_STDDELIVERYFILE.xml
[Fatal Error] _STDDELIVERYFILE.xml:1:1: Premature end of file.
2025-07-12T11:17:08.380+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Processing failed

org.xml.sax.SAXParseException: Premature end of file.
	at java.xml/com.sun.org.apache.xerces.internal.parsers.DOMParser.parse(DOMParser.java:262) ~[na:na]
	at java.xml/com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderImpl.parse(DocumentBuilderImpl.java:342) ~[na:na]
	at java.xml/javax.xml.parsers.DocumentBuilder.parse(DocumentBuilder.java:206) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:101) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:66) ~[classes!/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
