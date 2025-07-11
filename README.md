{kafka_offset=18707, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@40320673, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1752216760098, kafka_groupId=str-ecp-batch}]]
2025-07-11T08:52:40.291+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message.
2025-07-11T08:52:40.655+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📝 Metadata JSON before sending to OT:
{
  "BatchId" : "2c93525b-42d1-410a-9e26-aa957f19861d",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
  "UniqueECPBatchRef" : null,
  "Timestamp" : 1748351245,
  "RunPriority" : null,
  "EventType" : null,
  "EventID" : null,
  "RestartKey" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C4}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "/mnt/nfs/dev-exstream/dev-SA/input/DEBTMAN/2c93525b-42d1-410a-9e26-aa957f19861d/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "FileType" : "DATA",
    "ValidationStatus" : "valid",
    "ValidationRequirement" : null
  } ]
}
2025-07-11T08:52:40.688+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-07-11T08:52:40.688+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-07-11T08:52:40.688+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-07-11T08:52:40.960+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2FTrigger%2Fmetadata_2c93525b-42d1-410a-9e26-aa957f19861d.json'
2025-07-11T08:52:41.541+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🪪 OT JobId: e767d07f-6bfd-439a-9ec8-562f3b465519, InstanceId: 2c93525b-42d1-410a-9e26-aa957f19861d
2025-07-11T08:52:41.541+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for _STDDELIVERYFILE.xml in: /mnt/nfs/dev-exstream/dev-SA/jobs/e767d07f-6bfd-439a-9ec8-562f3b465519/2c93525b-42d1-410a-9e26-aa957f19861d/docgen
2025-07-11T08:52:41.557+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Failed in processing
java.nio.file.NoSuchFileException: /mnt/nfs/dev-exstream/dev-SA/jobs/e767d07f-6bfd-439a-9ec8-562f3b465519/2c93525b-42d1-410a-9e26-aa957f19861d/docgen
 at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
 at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
 at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
 at java.base/sun.nio.fs.UnixFileAttributeViews$Basic.readAttributes(UnixFileAttributeViews.java:55) ~[na:na]
