2025-06-03T16:14:04.464+02:00  INFO 32 --- [nio-8080-exec-3] c.n.k.f.service.BlobStorageService       : 🔍 Downloading blob: container='nsnakscontregecm001', blob='nsnakscontregecm001/NDDSST_250508.DAT'
2025-06-03T16:14:04.566+02:00 ERROR 32 --- [nio-8080-exec-3] c.n.k.f.service.BlobStorageService       : ❌ Error downloading blob content for 'nsnakscontregecm001/NDDSST_250508.DAT': Blob not found: nsnakscontregecm001/NDDSST_250508.DAT

com.nedbank.kafka.filemanage.exception.CustomAppException: Blob not found: nsnakscontregecm001/NDDSST_250508.DAT
	at com.nedbank.kafka.filemanage.service.BlobStorageService.downloadFileContent(BlobStorageService.java:256) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:170) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.listen(KafkaListenerService.java:89) ~[classes/:na]
	at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:30) ~[classes/:na]
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

public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();

            String extractedContainerName = containerName;  // default if container name is configured
            String blobName = blobPathOrUrl;

            // Handle full blob URL
            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");

                if (segments.length < 3) {
                    throw new CustomAppException("Invalid blob URL format: " + blobPathOrUrl, 400, HttpStatus.BAD_REQUEST);
                }

                // Example: /container/blob => segments[1] = container, segments[2...] = blob path
                extractedContainerName = segments[1];
                blobName = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            logger.info("🔍 Downloading blob: container='{}', blob='{}'", extractedContainerName, blobName);

            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(String.format("https://%s.blob.core.windows.net", accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(extractedContainerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            if (!blobClient.exists()) {
                throw new CustomAppException("Blob not found: " + blobName, 404, HttpStatus.NOT_FOUND);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            blobClient.download(outputStream);

            return outputStream.toString(StandardCharsets.UTF_8.name());

        } catch (Exception e) {
            logger.error("❌ Error downloading blob content for '{}': {}", blobPathOrUrl, e.getMessage(), e);
            throw new CustomAppException("Error downloading blob content", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
