# ===== Audit topic =====
kafka.topic.audit=log-ecp-batch-audit

# ===== Audit Kafka Producer Config =====
spring.kafka.producer.bootstrap-servers=nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# ===== SSL Settings =====
spring.kafka.producer.security.protocol=SSL
spring.kafka.producer.ssl.truststore-location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
spring.kafka.producer.ssl.truststore-password=nedbank1
spring.kafka.producer.ssl.keystore-location=C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
spring.kafka.producer.ssl.keystore-password=3dX7y3Yz9Jv6L4F
spring.kafka.producer.ssl.key-password=3dX7y3Yz9Jv6L4F

Audit topic message: 
 
BatchId	as received from kafka event
Servicename	Fmconsume
SystemEnv	DEV/ETE/QA/PROD azure (container name)
SourceSystem	DEBTMAN (as received from kafka event)
TenantCode	as received from kafka event
ChannelID	as received from kafka event
Product	as received from kafka event
Jobname	as received from kafka event
UniqueConsumerRef	as received from kafka event
Timestamp	 
RunPriority	as received from kafka event
EventType	 
StartTime	 
EndTime	 
BatchFiles	 
bloburl (incoming data file)	as received from kafka event
FileName	as received from kafka event
FileType	as received from kafka event
CustomerCount	counter in FM?

*********************
	
*******************

BatchId	as received from kafka event
Servicename	Fmcomplete
SystemEnv	DEV/ETE/QA/PROD azure (container name)
SourceSystem	DEBTMAN (as received from kafka event)
TenantCode	as received from kafka event
ChannelID	as received from kafka event
Product	as received from kafka event
Jobname	as received from kafka event
UniqueConsumerRef	as received from kafka event
Timestamp	 
RunPriority	as received from kafka event
EventType	 
StartTime	 
EndTime	 
BatchFiles	 
bloburl (incoming data file)	as received from kafka event
FileName	as received from kafka event
FileType	as received from kafka event
CustomerCount	counter in FM?
