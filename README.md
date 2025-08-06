# Kafka Consumer Configuration
kafka.bootstrap.servers=nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093
kafka.consumer.group.id=str-ecp-batch
kafka.consumer.auto.offset.reset=earliest
kafka.consumer.enable.auto.commit=false

# SSL Configuration
kafka.consumer.security.protocol=SSL
kafka.consumer.ssl.keystore.location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
kafka.consumer.ssl.keystore.password=3dX7y3Yz9Jv6L4F
kafka.consumer.ssl.key.password=3dX7y3Yz9Jv6L4F
kafka.consumer.ssl.truststore.location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
kafka.consumer.ssl.truststore.password=nedbank1
kafka.consumer.ssl.protocol=TLSv1.2

# Kafka Consumer Deserialization
kafka.consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
kafka.consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Kafka Producer Configuration (to send Summary File URL)
kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.producer.security.protocol=SSL
kafka.producer.ssl.keystore.location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
kafka.producer.ssl.keystore.password=3dX7y3Yz9Jv6L4F
kafka.producer.ssl.key.password=3dX7y3Yz9Jv6L4F
kafka.producer.ssl.truststore.location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
kafka.producer.ssl.truststore.password=nedbank1
kafka.producer.ssl.protocol=TLSv1.2
kafka.producer.bootstrap.servers=nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093
spring.kafka.producer.properties.request.timeout.ms=60000
spring.kafka.producer.properties.max.block.ms=180000
spring.kafka.producer.properties.metadata.max.age.ms=30000
spring.kafka.producer.properties.retries=5
spring.kafka.producer.properties.retry.backoff.ms=5000
# Allow long processing time (e.g., OT takes up to 1 hour)
spring.kafka.consumer.properties.max.poll.interval.ms=4200000
# Ensure only one message is polled at a time
spring.kafka.consumer.max-poll-records=1

azure.keyvault.uri=https://nsn-dev-ecm-kva-001.vault.azure.net/secrets

logging.level.org.springframework.kafka=DEBUG

kafka.topic.input=str-ecp-batch-composition
kafka.topic.output=str-ecp-batch-composition-complete

vault.hashicorp.url=https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
vault.hashicorp.namespace =admin/espire

vault.hashicorp.passwordDev=Dev+Cred4#
vault.hashicorp.passwordNbhDev=nbh_dev1

azure.blob.storage.account =https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001
azure.blob.storage.format=https://%s.blob.core.windows.net

azure.keyvault.url=https://nsn-dev-ecm-kva-001.vault.azure.net
azure.keyvault.accountKey=ecm-fm-account-key
azure.keyvault.accountName=ecm-fm-account-name
azure.keyvault.containerName=ecm-fm-container-name

mount.path=/mnt/nfs/dev-exstream/dev-SA
ot.orchestration.api.url=http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/batch/dev-SA/ECPDebtmanService
ot.service.mfc.url=http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/batch/dev-SA/MFCStatementService
otds.token.url=https://dev-exstream.nednet.co.za/otds/otdstenant/dev-exstream/otdsws/login
ot.auth.token=eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiI1ZjFkMzFjNC02ZTdkLTRlYWEtOTU3MC1hMGY4OWJiOGI3NTUiLCJzYXQiOjE3NTIyNDU2NTcsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiIxZmQ2YmI4NC00YjY0LTQzZDgtOTJiMS1kY2U2YWIzZDQ3OWYiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODM3ODE2NTcsImlhdCI6MTc1MjI0NTY1NywianRpIjoiMGU4ZWI4NzYtOWJmYi00OTczLWFiN2ItM2EyZTg4NWM5N2MzIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JdXQ7pDNlEBS8jOny0yhKrC85CsypDdJzjww_OhVKL4BNBLQRfJf04ESqcnoONEIfbeARLGPS6THMP6K6xOeHcO7oViTFtgXg27jhrfj6OXiU52pAvo2qFBAs6VvTueNjDOyQMsau-PzigYdPNw86IWzeK0Ude7DhaR1rNTPbu7LsqKHM3aD6SFli0EeLSux5eJYdWqTy2gpH4iNodxPjlyt5i6UoNEwl1TqUwbMEtbztfrGiwMPXvSflGBH10pSDDtNpssiyvsDl_flnqLmqxso-Ff5AVs8eAjHgsQnSEIeQQp9sX0JoSbNgW8D0iACdlI-6f9onOLg4JW-Ozucmg
otds.username=tenantadmin
otds.grant.type=password
otds.password=Exstream1!
otds.client-id=devexstreamclient
otds.client-secret=nV6A23zcFU5bK6lwm5KLBY2r5Sm3bh5l
rpt.max.wait.seconds=3600
rpt.poll.interval.millis=5000
kafka.listener.concurrency=3
source.system.debtman=DEBTMAN
source.system.mfc=MFC

proxy.host=proxyprod.africa.nedcor.net
proxy.port=80
proxy.username=CC437236
proxy.password=34dYaB@jEh56
use.proxy=false

