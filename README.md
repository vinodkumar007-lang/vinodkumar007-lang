2025-07-15T11:48:39.043+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ⏳ Waiting for XML for jobId=3eb92505-67eb-4db3-8c67-0f85a56ac693, id=8d3eed30-2996-4001-b7f9-379a5d87b2d2
2025-07-15T11:48:44.044+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-15T11:48:44.214+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/3eb92505-67eb-4db3-8c67-0f85a56ac693/8d3eed30-2996-4001-b7f9-379a5d87b2d2/docgen/07927ac7-53b9-4188-902c-14afc7c71e4c/output/_STDDELIVERYFILE.xml
[Fatal Error] _STDDELIVERYFILE.xml:1:1: Premature end of file.
2025-07-15T11:48:44.547+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
org.xml.sax.SAXParseException: Premature end of file.
 at java.xml/com.sun.org.apache.xerces.internal.parsers.DOMP
