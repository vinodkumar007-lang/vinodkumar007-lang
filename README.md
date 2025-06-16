2025-06-16T08:36:45.417+02:00  INFO 1 --- [nio-9091-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-06-16T08:36:45.417+02:00  INFO 1 --- [nio-9091-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-06-16T08:36:45.417+02:00  INFO 1 --- [nio-9091-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1750055805417
2025-06-16T08:36:45.417+02:00  INFO 1 --- [nio-9091-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-10, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-06-16T08:36:45.522+02:00  INFO 1 --- [nio-9091-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-10, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-06-16T08:36:45.522+02:00  INFO 1 --- [nio-9091-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-10, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-06-16T08:36:45.522+02:00  INFO 1 --- [nio-9091-exec-2] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-10, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-06-16T08:36:45.613+02:00  INFO 1 --- [nio-9091-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-10, groupId=str-ecp-batch] Seeking to offset 18619 for partition str-ecp-batch-composition-0
2025-06-16T08:36:45.729+02:00  INFO 1 --- [nio-9091-exec-2] c.n.k.f.service.BlobStorageService       : 🔐 Fetching secrets from Azure Key Vault...
2025-06-16T08:36:45.730+02:00  INFO 1 --- [nio-9091-exec-2] c.azure.identity.EnvironmentCredential   : Azure Identity => EnvironmentCredential invoking ClientSecretCredential
2025-06-16T08:36:45.730+02:00 ERROR 1 --- [nio-9091-exec-2] c.a.i.i.IntelliJCacheAccessor            : IntelliJ Authentication not available. Please log in with Azure Tools for IntelliJ plugin in the IDE.
2025-06-16T08:36:45.731+02:00  INFO 1 --- [nio-9091-exec-2] c.a.s.k.secrets.SecretAsyncClient        : Retrieving secret - ecm-fm-account-key
2025-06-16T08:36:46.486+02:00 ERROR 1 --- [     Thread-169] c.m.a.m.ConfidentialClientApplication    : [Correlation ID: f9d0bc8d-5420-4c5f-8ea6-fb3448065fb3] Execution of class com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier failed.

com.microsoft.aad.msal4j.MsalServiceException: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: a3f4fe66-6dee-4ad9-b520-811154271c00 Correlation ID: f9d0bc8d-5420-4c5f-8ea6-fb3448065fb3 Timestamp: 2025-06-16 06:36:46Z
	at com.microsoft.aad.msal4j.MsalServiceExceptionFactory.fromHttpResponse(MsalServiceExceptionFactory.java:43) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.createAuthenticationResultFromOauthHttpResponse(TokenRequestExecutor.java:96) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.executeTokenRequest(TokenRequestExecutor.java:37) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AbstractClientApplicationBase.acquireTokenCommon(AbstractClientApplicationBase.java:120) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier.execute(AcquireTokenByAuthorizationGrantSupplier.java:63) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:59) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:17) ~[msal4j-1.9.1.jar!/:1.9.1]
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-06-16T08:36:46.487+02:00 ERROR 1 --- [     Thread-169] c.azure.identity.ClientSecretCredential  : Azure Identity => ERROR in getToken() call for scopes [https://vault.azure.net/.default]: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: a3f4fe66-6dee-4ad9-b520-811154271c00 Correlation ID: f9d0bc8d-5420-4c5f-8ea6-fb3448065fb3 Timestamp: 2025-06-16 06:36:46Z
2025-06-16T08:36:47.489+02:00 ERROR 1 --- [     Thread-171] c.m.a.m.ConfidentialClientApplication    : [Correlation ID: 24bd3cb1-78a0-477f-b4df-8a1adaf8c235] Execution of class com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier failed.

com.microsoft.aad.msal4j.MsalServiceException: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: 2bcf383d-eb9a-4ef6-823b-8f5cb8e31600 Correlation ID: 24bd3cb1-78a0-477f-b4df-8a1adaf8c235 Timestamp: 2025-06-16 06:36:47Z
	at com.microsoft.aad.msal4j.MsalServiceExceptionFactory.fromHttpResponse(MsalServiceExceptionFactory.java:43) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.createAuthenticationResultFromOauthHttpResponse(TokenRequestExecutor.java:96) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.executeTokenRequest(TokenRequestExecutor.java:37) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AbstractClientApplicationBase.acquireTokenCommon(AbstractClientApplicationBase.java:120) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier.execute(AcquireTokenByAuthorizationGrantSupplier.java:63) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:59) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:17) ~[msal4j-1.9.1.jar!/:1.9.1]
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-06-16T08:36:47.490+02:00 ERROR 1 --- [     Thread-171] c.azure.identity.ClientSecretCredential  : Azure Identity => ERROR in getToken() call for scopes [https://vault.azure.net/.default]: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: 2bcf383d-eb9a-4ef6-823b-8f5cb8e31600 Correlation ID: 24bd3cb1-78a0-477f-b4df-8a1adaf8c235 Timestamp: 2025-06-16 06:36:47Z
2025-06-16T08:36:49.224+02:00 ERROR 1 --- [     Thread-173] c.m.a.m.ConfidentialClientApplication    : [Correlation ID: 641b114d-e63b-4493-902b-3739a3318d41] Execution of class com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier failed.

com.microsoft.aad.msal4j.MsalServiceException: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: 487fd45d-c182-43d2-aea9-8efc09b41400 Correlation ID: 641b114d-e63b-4493-902b-3739a3318d41 Timestamp: 2025-06-16 06:36:49Z
	at com.microsoft.aad.msal4j.MsalServiceExceptionFactory.fromHttpResponse(MsalServiceExceptionFactory.java:43) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.createAuthenticationResultFromOauthHttpResponse(TokenRequestExecutor.java:96) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.TokenRequestExecutor.executeTokenRequest(TokenRequestExecutor.java:37) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AbstractClientApplicationBase.acquireTokenCommon(AbstractClientApplicationBase.java:120) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier.execute(AcquireTokenByAuthorizationGrantSupplier.java:63) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:59) ~[msal4j-1.9.1.jar!/:1.9.1]
	at com.microsoft.aad.msal4j.AuthenticationResultSupplier.get(AuthenticationResultSupplier.java:17) ~[msal4j-1.9.1.jar!/:1.9.1]
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1768) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-06-16T08:36:49.225+02:00 ERROR 1 --- [     Thread-173] c.azure.identity.ClientSecretCredential  : Azure Identity => ERROR in getToken() call for scopes [https://vault.azure.net/.default]: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: 487fd45d-c182-43d2-aea9-8efc09b41400 Correlation ID: 641b114d-e63b-4493-902b-3739a3318d41 Timestamp: 2025-06-16 06:36:49Z
2025-06-16T08:36:52.744+02:00 ERROR 1 --- [     Thread-175] c.m.a.m.ConfidentialClientApplication    : [Correlation ID: 445970f1-7b8a-4340-acb5-86962bba74d5] Execution of class com.microsoft.aad.msal4j.AcquireTokenByAuthorizationGrantSupplier failed.
