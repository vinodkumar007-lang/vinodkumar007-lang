2025-07-01T12:27:47.532+02:00  INFO 1 --- [       Thread-1] c.azure.identity.DefaultAzureCredential  : Azure Identity => Attempted credential EnvironmentCredential returns a token
2025-07-01T12:27:47.927+02:00  INFO 1 --- [t.azure.net/...] c.a.s.k.secrets.SecretAsyncClient        : Retrieved secret - ecm-fm-account-key
2025-07-01T12:27:47.928+02:00  INFO 1 --- [ntainer#0-0-C-1] c.a.s.k.secrets.SecretAsyncClient        : Retrieving secret - ecm-fm-account-name
2025-07-01T12:27:48.056+02:00  INFO 1 --- [t.azure.net/...] c.a.s.k.secrets.SecretAsyncClient        : Retrieved secret - ecm-fm-account-name
2025-07-01T12:27:48.125+02:00  INFO 1 --- [ntainer#0-0-C-1] c.a.s.k.secrets.SecretAsyncClient        : Retrieving secret - ecm-fm-container-name
2025-07-01T12:27:48.265+02:00  INFO 1 --- [t.azure.net/...] c.a.s.k.secrets.SecretAsyncClient        : Retrieved secret - ecm-fm-container-name
2025-07-01T12:27:48.328+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ Secrets fetched successfully from Azure Key Vault.
2025-07-01T12:27:50.351+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📁 Saved DATA file to mount: /mnt/nfs/dev-exstream/dev-SA/jobs/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN.csv
2025-07-01T12:27:50.432+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 Sending metadata.json to OpenText API at: http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService
2025-07-01T12:27:52.774+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Kafka message processed and sent to OT: Sent metadata to OT
