# Kafka Consumer Configuration
kafka.bootstrap.servers=nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093
kafka.consumer.group.id=str-ecp-batch
kafka.consumer.auto.offset.reset=earliest

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

logging.level.com.azure.spring=DEBUG

kafka.topic.input=str-ecp-batch-composition
kafka.topic.output=ecp-batch-composition-complete

vault.hashicorp.url=https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
vault.hashicorp.namespace =admin/espire

vault.hashicorp.passwordDev=Dev+Cred4#
vault.hashicorp.passwordNbhDev=nbh_dev1
