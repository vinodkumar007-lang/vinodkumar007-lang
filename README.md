2025-07-16T05:48:22.294+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 🚀 Calling Orchestration API: http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/batch/dev-SA/ECPDebtmanService
2025-07-16T05:48:22.688+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Failed OT Orchestration call
org.springframework.web.client.HttpServerErrorException$InternalServerError: 500 : "{"timestamp":1752637702607,"status":500,"message":"Internal Server Error","details":"java.lang.RuntimeException: org.apache.hc.client5.http.HttpHostConnectException: Connect to http://experience-deployment-otdsws.dev-exstream:80 [experience-deployment-otdsws.dev-exstream/158.2.126.164] failed: Connection refused"}"
 at org.springframework.web.client.HttpServerErrorException.create(HttpServerErrorException.java:103) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:186) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:137) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.ResponseErrorHandler.handleError(ResponseErrorHandler.java:63) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.handleResponse(RestTemplate.java:915) ~[spring-web-6.0.2.jar!/:6.0.2]
