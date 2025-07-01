2025-07-01T06:27:14.174+02:00  INFO 1 --- [t.azure.net/...] c.a.s.k.secrets.SecretAsyncClient        : Retrieved secret - ecm-fm-container-name
2025-07-01T06:27:14.174+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ Secrets fetched successfully from Azure Key Vault.
2025-07-01T06:27:16.038+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📁 Saved DATA file to mount: /mnt/nfs/dev-exstream/dev-SA/jobs/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN.csv
2025-07-01T06:27:16.149+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 Sending metadata.json to OpenText API at: http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService
2025-07-01T06:27:16.643+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Failed to send metadata.json to OT [batchId=2c93525b-42d1-410a-9e26-aa957f19861d, guiRef=6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f]
org.springframework.web.client.HttpClientErrorException$Unauthorized: 401 : [no body]
 at org.springframework.web.client.HttpClientErrorException.create(HttpClientErrorException.java:106) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:183) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:137) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.ResponseErrorHandler.handleError(ResponseErrorHandler.java:63) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.handleResponse(RestTemplate.java:915) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:864) ~[spring-web-6.0.2.jar!/:6.0.2]
