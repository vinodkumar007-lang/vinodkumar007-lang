2025-06-03T16:27:06.933+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-06-03T16:27:06.933+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-06-03T16:27:06.933+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748960826933
2025-06-03T16:27:06.938+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-06-03T16:27:07.970+02:00  INFO 5116 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-06-03T16:27:07.973+02:00  INFO 5116 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-06-03T16:27:07.974+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-06-03T16:27:08.064+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Seeking to offset 18580 for partition str-ecp-batch-composition-0
2025-06-03T16:27:08.303+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : Vault secrets initialized for Blob Storage
2025-06-03T16:27:08.304+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : 🔍 Downloading blob: container='nsnakscontregecm001', blob='NDDSST_250508.DAT'
2025-06-03T16:27:10.154+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : Built print file URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/1970-01-21/78d63508-9603-46ad-b82a-4fe7528fde60/c2c005b2-5e5d-45a1-a7c9-5cb2f6fc8d8f/DEBTMAN/print/78d63508-9603-46ad-b82a-4fe7528fde60_printfile.pdf
2025-06-03T16:27:10.179+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON successfully written to file: C:\Users\CC437236\AppData\Local\Temp\summary-14556320968912805868.json
2025-06-03T16:27:10.332+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F78d63508-9603-46ad-b82a-4fe7528fde60%2Fc2c005b2-5e5d-45a1-a7c9-5cb2f6fc8d8f%2Fsummary.json'
2025-06-03T16:27:10.332+02:00  INFO 5116 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : Uploaded summary JSON to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F78d63508-9603-46ad-b82a-4fe7528fde60%2Fc2c005b2-5e5d-45a1-a7c9-5cb2f6fc8d8f%2Fsummary.json'
2025-06-03T16:27:10.348+02:00  INFO 5116 --- [nio-8080-exec-2] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 

{
    "message": null,
    "status": null,
    "data": null,
    "summaryPayload": null
}
