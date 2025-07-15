2025-07-15T10:56:06.539+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ✅ Found XML file: /mnt/nfs/dev-exstream/dev-SA/jobs/d31e5fe2-bfca-41a9-9465-ce401e8fac2d/6684a60c-78f8-4ee5-ae93-d09a8dfcd304/docgen/8c732e88-a80d-4dbc-b2a7-e265846726f5/output/_STDDELIVERYFILE.xml
2025-07-15T10:56:09.666+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-15T10:56:14.735+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-07-15T10:56:16.236+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📄 Extracted 8853 customers from XML
2025-07-15T10:56:16.250+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📑 Parsed error report with 0 entries
2025-07-15T10:56:16.850+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
java.lang.NullPointerException: null
 at java.base/java.util.Objects.requireNonNull(Objects.java:209) ~[na:na]
 at java.base/java.util.ImmutableCollections.listFromTrustedArray(ImmutableCollections.java:213) ~[na:na]
 at java.base/ja
