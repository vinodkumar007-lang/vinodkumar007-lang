2025-07-11T12:23:00.659+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2FTrigger%2Fmetadata_2c93525b-42d1-410a-9e26-aa957f19861d.json'
2025-07-11T12:23:01.125+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📨 OT Orchestration Response: {
  "status" : "success",
  "data" : [ {
    "id" : "6511a596-e0b2-446b-9e27-52d41952532a",
    "jobId" : "235914ec-845b-48e5-818f-5939dd9e65de",
    "flowModelId" : "c1dc4796-38f8-469d-9770-b64cf3653258",
    "flowModelSnapshotId" : "bdfaed3a-b3d1-4aa9-9683-01e05204a0a1",
    "domainId" : "dev-SA",
    "status" : "started",
    "startDate" : 1752229380998,
    "endDate" : null,
    "expirationDate" : null,
    "externalId" : null,
    "flowModelType" : "COMMUNICATION",
    "executingLongRunningOp" : false,
    "flowModelName" : null
  } ]
}
2025-07-11T12:23:01.125+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🔎 Searching for _STDDELIVERYFILE.xml in /mnt/nfs/dev-exstream/dev-SA/jobs/235914ec-845b-48e5-818f-5939dd9e65de/235914ec-845b-48e5-818f-5939dd9e65de/docgen
2025-07-11T12:23:01.142+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Waiting for docgen folder... retrying in 5000ms
2025-07-11T12:23:01.344+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-07-11T12:23:01.344+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-07-11T12:23:01.344+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-07-11T12:23:02.499+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-07-11T12:23:02.499+02:00  WARN 1 --- [ad | producer-1] org.apache.ka
