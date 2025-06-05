025-06-05T17:54:02.009+02:00  WARN 1 --- [       Thread-7] c.a.s.k.secrets.SecretAsyncClient        : Failed to get secret - account-key

Max retries 3 times exceeded. Error Details: EnvironmentCredential authentication unavailable. Environment variables are not fully configured.

ManagedIdentityCredential authentication unavailable. Connection to IMDS endpoint cannot be established.

SharedTokenCacheCredential authentication unavailable. No accounts were found in the cache.

IntelliJ Authentication not available. Please log in with Azure Tools for IntelliJ plugin in the IDE.

Failed to read Vs Code credentials from Linux Key Ring.

AzureCliCredential authentication unavailable. Azure CLI not installed

2025-06-05T17:54:02.010+02:00 ERROR 1 --- [nio-9091-exec-5] c.n.k.f.service.BlobStorageService       : ❌ Failed to fetch secret 'account-key': Max retries 3 times exceeded. Error Details: EnvironmentCredential authentication unavailable. Environment variables are not fully configured.

ManagedIdentityCredential authentication unavailable. Connection to IMDS endpoint cannot be established.

SharedTokenCacheCredential authentication unavailable. No accounts were found in the cache.

IntelliJ Authentication not available. Please log in with Azure Tools for IntelliJ plugin in the IDE.

Failed to read Vs Code credentials from Linux Key Ring.

AzureCliCredential authentication unavailable. Azure CLI not installed

 
What are you doing with Azure CLI?
 
 
Caused by: com.azure.identity.CredentialUnavailableException: EnvironmentCredential authentication unavailable. Environment variables are not fully configured.

ManagedIdentityCredential authentication unavailable. Connection to IMDS endpoint cannot be established.

SharedTokenCacheCredential authentication unavailable. No accounts were found in the cache.

IntelliJ Authentication not available. Please log in with Azure Tools for IntelliJ plugin in the IDE.

Failed to read Vs Code credentials from Linux Key Ring.

AzureCliCredential authentication unavailable. Azure CLI not installed

        at com.azure.identity.ChainedTokenCredential.lambda$getToken$3(ChainedTokenCredential.java:77) ~[azure-identity-1.2.5.jar!/:na]

        at reactor.core.publisher.MonoDefer.subscribe(MonoDefer.java:44) ~[reactor-core-3.4.25.jar!/:3.4.25]

        ... 28 common frames omitted
 
2025-06-05T17:54:02.117+02:00 ERROR 1 --- [nio-9091-exec-5] c.n.k.f.service.BlobStorageService       : ❌ Failed to initialize secrets from Key Vault: Failed to fetch secret: account-key
 
com.nedbank.kafka.filemanage.exception.CustomAppException: Failed to fetch secret: account-key

        at com.nedbank.kafka.filemanage.service.BlobStorageService.getSecret(BlobStorageService.java:85) ~[classes!/:na]

        at com.nedbank.kafka.filemanage.service.BlobStorageService.initSecrets(BlobStorageService.java:62) ~[classes!/:na]

        at com.nedbank.kafka.filemanage.service.BlobStorageService.buildPrintFileUrl(BlobStorageService.java:219) ~[classes!/:na]

        at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:328) ~[classes!/:na]

        at com.nedbank.kafka.filemanage.service.KafkaListenerService.listen(KafkaListenerService.java:127) ~[classes!/:na]

        at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:33) ~[classes!/:na]

        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]

        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]

        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]

        at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]

        at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:207) ~[spring-web-6.0.2.jar!/:6.0.2]

        at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:152) ~[spring-web-6.0.2.jar!/:6.0.2]

        at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:117) ~[spring-webmvc-6.0.2.jar!/:6.0.2]

        at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:884) ~[spring-webmvc-6.0.2.jar!/:6.0.2]

        at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:797) ~[spring-webmvc-6.0.2.jar!/:6.0.2]

        at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87) ~[spring-webmvc-6.0.2.j
 
I am trying to fetch secrets from keyvalult by azure SDK.
 
Was this discussed with securit? is a credential used for this?
 
My main problem is , when you hit , logs getting generated, but when i am trying getting 404.
 
 
It was already discussed with Wayne, 
Mashaba, M. (Mfumo)
Was this discussed with securit? is a credential used for this?
It was already discussed with Wayne , Philip and Security team in past to use Azure Key Vault for the account key credentials, 
 
which credential is used to fetch this?
 
we are not using any credentials , we are passing keyvault url to get SecretClient, from SecretClient object trying to get secrets.
SecretClient secretClient = new SecretClientBuilder()        .vaultUrl(keyVaultUrl)        .credential(new DefaultAzureCredentialBuilder().build())        .buildClient();
 
Mashaba, M. (Mfumo)
Caused by: com.azure.identity.CredentialUnavailableException: EnvironmentCredential authentication unavailable. Environment variables are not fully configured.  ManagedIdentityCredential authentication unavailable. Connection to IMDS endpoint cannot be established.  SharedTokenCacheCredential authe…
If you can look at the logs you will see it's failing to fetch the secrets 
 
