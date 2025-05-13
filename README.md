09:09:04.487 [main] DEBUG org.springframework.web.client.RestTemplate - HTTP GET https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health
09:09:04.518 [main] DEBUG org.springframework.web.client.RestTemplate - Accept=[text/plain, application/xml, text/xml, application/json, application/*+xml, application/*+json, */*]
09:09:05.218 [main] DEBUG org.springframework.web.client.RestTemplate - Response 473
Exception in thread "main" org.springframework.web.client.HttpClientErrorException: 473 status code 473: "{"initialized":true,"sealed":false,"standby":true,"performance_standby":true,"replication_performance_mode":"disabled","replication_dr_mode":"disabled","server_time_utc":1747120145,"version":"1.18.4+ent","enterprise":true,"cluster_name":"ca5012a3-68e3-4b29-ab44-dc12be06bd82","cluster_id":"814be235-c399-6748-76e7-f0df2356ad27","license":{"state":"autoloaded","expiry_time":"2025-06-23T21:25:10Z","terminated":false},"echo_duration_ms":0,"clock_skew_ms":0,"replication_primary_canary_age_ms":413}<EOL>"
	at org.springframework.web.client.HttpClientErrorException.create(HttpClientErrorException.java:141)
	at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:183)
	at org.springframework.web.client.DefaultResponseErrorHandler.handleError(DefaultResponseErrorHandler.java:137)
	at org.springframework.web.client.ResponseErrorHandler.handleError(ResponseErrorHandler.java:63)
	at org.springframework.web.client.RestTemplate.handleResponse(RestTemplate.java:915)
	at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:864)
	at org.springframework.web.client.RestTemplate.execute(RestTemplate.java:764)
	at org.springframework.web.client.RestTemplate.getForObject(RestTemplate.java:378)
	at com.nedbank.kafka.filemanage.service.VaultService.checkVaultHealth(VaultService.java:19)
