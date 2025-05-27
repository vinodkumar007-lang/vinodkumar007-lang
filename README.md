2025-05-27T15:54:26.912+02:00  INFO 17488 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Processing message from topic str-ecp-batch-composition, partition=0, offset=18531
2025-05-27T15:54:27.040+02:00  INFO 17488 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Processing batchId=1c93525b-42d1-410a-9e26-aa957f19861a, sourceSystem=DEBTMAN
2025-05-27T15:54:27.045+02:00  INFO 17488 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Copying file from 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv' to 'DEBTMAN/input/2025-05-27T13-07-25Z/1c93525b-42d1-410a-9e26-aa957f19861a/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f_DEBTMAN/DEBTMAN.csv'
2025-05-27T15:54:27.046+02:00  INFO 17488 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : Vault secrets initialized for Blob Storage
2025-05-27T15:54:27.910+02:00 ERROR 17488 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ❌ Error copying file from URL: 409 Public access is not permitted on this storage account.: "﻿<?xml version="1.0" encoding="utf-8"?><Error><Code>PublicAccessNotPermitted</Code><Message>Public access is not permitted on this storage account.<EOL>RequestId:c992ee3b-c01e-006e-710e-cffdb8000000<EOL>Time:2025-05-27T13:54:27.8648382Z</Message></Error>"

org.springframework.web.client.HttpClientErrorException$Conflict: 409 Public access is not permitted on this storage account.: "﻿<?xml version="1.0" encoding="utf-8"?><Error><Code>PublicAccessNotPermitted</Code><Message>Public access is not permitted on this storage account.<EOL>RequestId:c992ee3b-c01e-006e-710e-cffdb8000000<EOL>Time:2025-05-27T13:54:27.8648382Z</Message></Error>"
	at org.springframework.web.client.HttpClientErrorException.create(HttpClientErrorException.java:121) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:183) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:137) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.ResponseErrorHandler.handleError(ResponseErrorHandler.java:63) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.RestTemplate.handleResponse(RestTemplate.java:915) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:864) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.client.RestTemplate.execute(RestTemplate.java:764) ~[spring-web-6.0.2.jar:6.0.2]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.copyFileFromUrlToBlob(BlobStorageService.java:80) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:159) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.listen(KafkaListenerService.java:86) ~[classes/:na]
	at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:32) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:207) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:152) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:117) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:884) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:797) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1080) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:973) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1003) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.FrameworkServlet.doPost(FrameworkServlet.java:906) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:731) ~[tomcat-embed-core-10.1.1.jar:6.0]
	at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:880) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:814) ~[tomcat-embed-core-10.1.1.jar:6.0]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:223) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:53) ~[tomcat-embed-websocket-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:201) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:197) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:97) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:542) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:119) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:92) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:78) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:357) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:400) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:65) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:861) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1739) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:52) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1191) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:659) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61) ~[tomcat-embed-core-1

  public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient targetBlobClient = containerClient.getBlobClient(targetBlobPath);

            InputStream inputStream = restTemplate.execute(sourceUrl, HttpMethod.GET, null, clientHttpResponse -> {
                if (clientHttpResponse.getStatusCode() != HttpStatus.OK) {
                    throw new CustomAppException("Failed to fetch source file, status: " + clientHttpResponse.getStatusCode(), 404, HttpStatus.NOT_FOUND);
                }
                return clientHttpResponse.getBody();
            });

            if (inputStream == null) {
                throw new CustomAppException("Unable to read source file from URL: " + sourceUrl, 404, HttpStatus.NOT_FOUND);
            }

            // Upload the stream directly without buffering entire file in memory
            targetBlobClient.upload(inputStream, -1, true);  // -1 means unknown length, stream upload

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, targetBlobClient.getBlobUrl());

            return targetBlobClient.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error copying file from URL: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying file from URL", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
