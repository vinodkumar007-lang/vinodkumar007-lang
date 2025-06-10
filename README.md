com.nedbank.kafka.filemanage.exception.CustomAppException: Failed to fetch secret: ecm-fm-account-key
        at com.nedbank.kafka.filemanage.service.BlobStorageService.getSecret(BlobStorageService.java:86) ~[classes!/:na]
        at com.nedbank.kafka.filemanage.service.BlobStorageService.initSecrets(BlobStorageService.java:63) ~[classes!/:na]
        ... 55 common frames omitted
Caused by: java.lang.RuntimeException: Max retries 3 times exceeded. Error Details: DefaultAzureCredential authentication failed. ---> EnvironmentCredential authentication failed. Error Details: AADSTS700016: Application with identifier 'f8b3e641-5baa-4a97-b58d-a7deecc0f5c2' was not found in the directory 'Nedbank'. This can happen if the application has not been installed by the administrator of the tenant or consented to by any user in the tenant. You may have sent your authentication request to the wrong tenant. Trace ID: b8a1ae82-42fe-4e3c-b3a5-f754b6db0c00 Correlation ID: 78273951-b1fe-4313-8702-5a4f6d953ddb Timestamp: 2025-06-10 12:31:56Z
 
