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
otds.token.url=https://dev-exstream.nednet.co.za/otds/otdstenant/dev-exstream/otdsws/login
otds.username=tenantadmin
otds.grant.type=password
otds.password=Exstream1!
otds.client-id=devexstreamclient
otds.client-secret=nV6A23zcFU5bK6lwm5KLBY2r5Sm3bh5l
rpt.max.wait.seconds=3600
rpt.poll.interval.millis=5000

kafka.ssl.disable.hostname.verification= ;

proxy.host=proxyprod.africa.nedcor.net
proxy.port=80
proxy.username=CC437236
proxy.password=34dYaB@jEh56
use.proxy=false

