2025-06-04T05:27:43.924+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-06-04T05:27:43.924+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-06-04T05:27:43.925+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1749007663924
2025-06-04T05:27:43.925+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-06-04T05:27:44.691+02:00  INFO 23368 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-06-04T05:27:44.694+02:00  INFO 23368 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-06-04T05:27:44.697+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-06-04T05:27:44.793+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Seeking to offset 18589 for partition str-ecp-batch-composition-0
2025-06-04T05:27:45.151+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : Vault secrets initialized for Blob Storage
2025-06-04T05:27:45.151+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : 🔍 Downloading blob: container='nsnakscontregecm001', blob='DEBTMAN.csv'
2025-06-04T05:27:46.198+02:00  WARN 23368 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : No customers extracted from file DEBTMAN.csv
2025-06-04T05:27:46.198+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Total processed files count: 0
2025-06-04T05:27:46.200+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : Built print file URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/1970-01-21/1c93525b-42d1-410a-9e26-aa957f19861c/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN/print/1c93525b-42d1-410a-9e26-aa957f19861c_printfile.pdf
2025-06-04T05:27:46.232+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON successfully written to file: C:\Users\CC437236\AppData\Local\Temp\summary-16376776516695564289.json
2025-06-04T05:27:46.384+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Fsummary.json'
2025-06-04T05:27:46.385+02:00  INFO 23368 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : Uploaded summary JSON to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Fsummary.json'
2025-06-04T05:27:46.402+02:00  INFO 23368 --- [nio-8080-exec-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
