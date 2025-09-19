Dockerfile:

FROM redhat.ncr.devops.nednet.co.za/ubi8/openjdk-17-runtime@sha256:763507bf338323e15bfc1e8913bb1daa76c724642d0c4e65c2c7682dc87df144

USER root

WORKDIR /app

# Set the correct time zone

ENV TZ=Africa/Johannesburg

ENV JAVA_OPTS_APPEND="-Duser.timezone=Africa/Johannesburg -Duser.language=en -Duser.country=ZA -Xms256m -Xmx512m"

ENV KEYSTORE_PASS=changeit

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY Docker/certs /tmp/certs
 
RUN keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteissuing -file "/tmp/certs/NedETE_Issuing_Sha2 1.cer" -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias etepolicy -file "/tmp/certs/NedETE_Policy_Sha2 1.cer" -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteroot -file "/tmp/certs/NedETE_Root_Sha2 1.cer" -noprompt \
&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt \
&& rm -rf /tmp/certs
 
RUN chown -R jboss:jboss /app \
&& chown -R jboss:jboss /mnt

USER jboss

COPY target/*.jar /deployments/
 
application.properties

# Kafka Consumer Configuration
kafka.bootstrap.servers=${BOOTSTRAP_SERVERS}
kafka.consumer.group.id=str-ecp-batch
kafka.consumer.auto.offset.reset=earliest
kafka.consumer.enable.auto.commit=false

# SSL Configuration
kafka.consumer.security.protocol=SSL
kafka.consumer.ssl.keystore.location=${KAFKA_KEYSTORE}
kafka.consumer.ssl.keystore.password=${KEYSTORE_PASSWORD}
kafka.consumer.ssl.key.password=${KEYSTORE_PASSWORD}
kafka.consumer.ssl.truststore.location=${KAFKA_TRUSTSTORE}
kafka.consumer.ssl.truststore.password=${TRUSTSTORE_PASSWORD}
kafka.consumer.ssl.protocol=TLSv1.2

# Kafka Consumer Deserialization
kafka.consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
kafka.consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Kafka Producer Configuration (to send Summary File URL)
kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.producer.security.protocol=SSL
kafka.producer.ssl.keystore.location=${KAFKA_KEYSTORE}
kafka.producer.ssl.keystore.password=${KEYSTORE_PASSWORD}
kafka.producer.ssl.key.password=${KEYSTORE_PASSWORD}
kafka.producer.ssl.truststore.location=${KAFKA_TRUSTSTORE}
kafka.producer.ssl.truststore.password=${TRUSTSTORE_PASSWORD}
kafka.producer.ssl.protocol=TLSv1.2
kafka.producer.bootstrap.servers=${BOOTSTRAP_SERVERS}
spring.kafka.producer.properties.request.timeout.ms=60000
spring.kafka.producer.properties.max.block.ms=180000
spring.kafka.producer.properties.metadata.max.age.ms=30000
spring.kafka.producer.properties.retries=5
spring.kafka.producer.properties.retry.backoff.ms=5000
# Allow long processing time (e.g., OT takes up to 1 hour)
spring.kafka.consumer.properties.max.poll.interval.ms=4200000
# Ensure only one message is polled at a time
spring.kafka.consumer.max-poll-records=1
azure.keyvault.uri=${KEYVAULT_URL}
logging.level.org.springframework.kafka=DEBUG
kafka.topic.input=str-ecp-batch-composition
kafka.topic.output=str-ecp-batch-composition-complete
azure.blob.storage.account=${STORAGE_ACCOUNT}
azure.keyvault.url=${AZURE_KEYVAULT_URL}
spring.profiles.active=${SPRING_PROFILE_ENV}
server.port=9091
azure.blob.storage.format=${BLOB_STORAGE_FORMAT}
azure.keyvault.accountKey=${KEYVAULT_ACCOUNTKEY}
azure.keyvault.accountName=${KEYVAULT_ACCOUNTNAME}
azure.keyvault.containerName=${KEYVAULT_CONTAINERNAME}
# ot.auth.token=${OTDS_TOKEN_ETE}
source.systems[0].name=DEBTMAN
source.systems[0].url=${ORCHESTRATION_URL}
source.systems[0].token=otds-token-dev

source.systems[1].name=MFC
source.systems[1].url=${OT_SERVICE_MFC_URL}
source.systems[0].token=otds-token-dev
mount.path=${SOURCE_PATH}
ot.orchestration.api.url=${ORCHESTRATION_URL}
#ot.service.mfc.url=${OT_SERVICE_MFC_URL}
# Orchestration runtime base URL
ot.runtime.url=${OT_RUNTIME_URL}
rpt.max.wait.seconds=3600
rpt.poll.interval.millis=5000
kafka.listener.concurrency=3
# ==== Audit Kafka Producer Settings ====
kafka.topic.audit=log-ecp-batch-audit
audit.kafka.producer.bootstrap.servers=nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093
audit.kafka.producer.security.protocol=SSL
audit.kafka.producer.ssl.truststore.location=${KAFKA_TRUSTSTORE}
audit.kafka.producer.ssl.truststore.password=${TRUSTSTORE_PASSWORD}
audit.kafka.producer.ssl.keystore.location=${KAFKA_KEYSTORE}
audit.kafka.producer.ssl.keystore.password=${KEYSTORE_PASSWORD}
audit.kafka.producer.ssl.key.password=${KEYSTORE_PASSWORD}
audit.kafka.producer.ssl.protocol=TLSv1.2
