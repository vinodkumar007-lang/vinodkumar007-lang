2025-10-27T12:33:29.152+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Using URL=http://exstream-deployment-orchestration-service.dev-exstream.svc:8300/orchestration/api/v1/inputs/batch/dev-SA/CADNT1Service for NEDTRUST:CADNT1 with token=otds-token-dev
2025-10-27T12:33:29.156+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 🚀 [batchId: 46cb192f-e3b0-4ced-87b7-722b6b20f58a] Calling Orchestration API attempt 1: http://exstream-deployment-orchestration-service.dev-exstream.svc:8300/orchestration/api/v1/inputs/batch/dev-SA/CADNT1Service
2025-10-27T12:33:29.157+02:00  INFO 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : 📡 Initiating OT orchestration call to URL: http://exstream-deployment-orchestration-service.dev-exstream.svc:8300/orchestration/api/v1/inputs/batch/dev-SA/CADNT1Service for batchId: 46cb192f-e3b0-4ced-87b7-722b6b20f58a and sourceSystem: NEDTRUST
2025-10-27T12:33:29.951+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-10-27T12:33:29.951+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-10-27T12:33:30.262+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Exception during OT orchestration call for batchId: 46cb192f-e3b0-4ced-87b7-722b6b20f58a - 401 : [no body]
org.springframework.web.client.HttpClientErrorException$Unauthorized: 401 : [no body]
 at org.springframework.web.client.HttpClientErrorException.create(HttpClientErrorException.java:106) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:183) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:137) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.ResponseErrorHandler.handleError(ResponseErrorHandler.java:63) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.handleResponse(RestTemplate.java:915) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:864) ~[spring-web-6.0.2.jar!/:6.0.2]
