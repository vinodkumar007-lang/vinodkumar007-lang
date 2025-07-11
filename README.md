2025-07-11T14:39:25.639+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2FTrigger%2Fmetadata_2c93525b-42d1-410a-9e26-aa957f19861d.json'
2025-07-11T14:39:26.951+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📨 OT Orchestration Response: {
  "status" : "success",
  "data" : [ {
    "id" : "50d2d1e3-2bf5-4455-beb2-26d87b62db21",
    "jobId" : "0714f080-c15e-4e30-acc3-fcd051a6e0f2",
    "flowModelId" : "c1dc4796-38f8-469d-9770-b64cf3653258",
    "flowModelSnapshotId" : "bdfaed3a-b3d1-4aa9-9683-01e05204a0a1",
    "domainId" : "dev-SA",
    "status" : "started",
    "startDate" : 1752237566814,
    "endDate" : null,
    "expirationDate" : null,
    "externalId" : null,
    "flowModelType" : "COMMUNICATION",
    "executingLongRunningOp" : false,
    "flowModelName" : null
  } ]
}
2025-07-11T14:39:26.951+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🔎 Searching for _STDDELIVERYFILE.xml under /mnt/nfs/dev-exstream/dev-SA/jobs/0714f080-c15e-4e30-acc3-fcd051a6e0f2/50d2d1e3-2bf5-4455-beb2-26d87b62db21/docgen
2025-07-11T14:39:26.967+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📂 docgen folder not found. Retrying in 5000ms...
2025-07-11T14:39:31.967+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📂 docgen folder not found. Retrying in 5000ms...
2025-07-11T14:39:36.968+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📂 docgen folder not found. Retrying in 5000ms...
