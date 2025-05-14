2025-05-14T14:37:03.542+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : str-ecp-batch: partitions assigned: [str-ecp-batch-composition-0]
2025-05-14T14:37:03.768+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 500 records
2025-05-14T14:37:03.784+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@3e590344, headers={id=daa36397-8a9b-472c-4370-bb3bb3a60c48, timestamp=1747226220378}]]
2025-05-14T14:37:03.785+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T14:37:03.785+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747084595.937577800,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-14T14:37:03.816+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Parsed batchId: 12345, filePath: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv, objectId: {1037A096-0000-CE1A-A484-3290CA7938C2}
2025-05-14T14:37:05.384+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ File uploaded successfully from 'DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv'
2025-05-14T14:37:05.391+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 🔐 SAS URL (valid for 24 hours): https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.391+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : File uploaded to blob storage at URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.445+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
	batch.size = 16384
	bootstrap.servers = [nsnxeteelpka01.nednet.co.za:9093, nsnxeteelpka02.nednet.co.za:9093, nsnxeteelpka03.nednet.co.za:9093]
	buffer.memory = 33554432
	client.dns.lookup = use_all_dns_ips
	client.id = producer-1
	compression.type = none
	connections.max.idle.ms = 540000
	delivery.timeout.ms = 120000
	enable.idempotence = true
	interceptor.classes = []
	key.serializer = class org.apache.kafka.common.serialization.StringSerializer
	linger.ms = 0
	max.block.ms = 60000
	max.in.flight.requests.per.connection = 5
	max.request.size = 1048576
	metadata.max.age.ms = 300000
	metadata.max.idle.ms = 300000
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	partitioner.adaptive.partitioning.enable = true
	partitioner.availability.timeout.ms = 0
	partitioner.class = null
	partitioner.ignore.keys = false
	receive.buffer.bytes = 32768
	reconnect.backoff.max.ms = 1000
	reconnect.backoff.ms = 50
	request.timeout.ms = 30000
	retries = 2147483647
	retry.backoff.ms = 100
	sasl.client.callback.handler.class = null
	sasl.jaas.config = null
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.min.time.before.relogin = 60000
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	sasl.kerberos.ticket.renew.window.factor = 0.8
	sasl.login.callback.handler.class = null
	sasl.login.class = null
	sasl.login.connect.timeout.ms = null
	sasl.login.read.timeout.ms = null
	sasl.login.refresh.buffer.seconds = 300
	sasl.login.refresh.min.period.seconds = 60
	sasl.login.refresh.window.factor = 0.8
	sasl.login.refresh.window.jitter = 0.05
	sasl.login.retry.backoff.max.ms = 10000
	sasl.login.retry.backoff.ms = 100
	sasl.mechanism = GSSAPI
	sasl.oauthbearer.clock.skew.seconds = 30
	sasl.oauthbearer.expected.audience = null
	sasl.oauthbearer.expected.issuer = null
	sasl.oauthbearer.jwks.endpoint.refresh.ms = 3600000
	sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms = 10000
	sasl.oauthbearer.jwks.endpoint.retry.backoff.ms = 100
	sasl.oauthbearer.jwks.endpoint.url = null
	sasl.oauthbearer.scope.claim.name = scope
	sasl.oauthbearer.sub.claim.name = sub
	sasl.oauthbearer.token.endpoint.url = null
	security.protocol = SSL
	security.providers = null
	send.buffer.bytes = 131072
	socket.connection.setup.timeout.max.ms = 30000
	socket.connection.setup.timeout.ms = 10000
	ssl.cipher.suites = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.3]
	ssl.endpoint.identification.algorithm = https
	ssl.engine.factory.class = null
	ssl.key.password = [hidden]
	ssl.keymanager.algorithm = SunX509
	ssl.keystore.certificate.chain = null
	ssl.keystore.key = null
	ssl.keystore.location = C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\keystore.jks
	ssl.keystore.password = [hidden]
	ssl.keystore.type = JKS
	ssl.protocol = TLSv1.2
	ssl.provider = null
	ssl.secure.random.implementation = null
	ssl.trustmanager.algorithm = PKIX
	ssl.truststore.certificates = null
	ssl.truststore.location = C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\truststore.jks
	ssl.truststore.password = [hidden]
	ssl.truststore.type = JKS
	transaction.timeout.ms = 60000
	transactional.id = null
	value.serializer = class org.apache.kafka.common.serialization.StringSerializer

2025-05-14T14:37:05.473+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-05-14T14:37:05.560+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-14T14:37:05.560+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-14T14:37:05.560+02:00  INFO 5856 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1747226225560
2025-05-14T14:37:05.561+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@7c9aa66d]
2025-05-14T14:37:05.651+02:00  INFO 5856 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition str-ecp-batch-composition-complete-0 to 30 since the associated topicId changed from null to kFgHvXbjT6KB5Z2coZAcJw
2025-05-14T14:37:05.652+02:00  INFO 5856 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-14T14:37:05.654+02:00  INFO 5856 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 194032 with epoch 0
2025-05-14T14:37:05.695+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Summary published to Kafka topic: str-ecp-batch-composition-complete with message: {"batchID":"12345","header":{"tenantCode":"ZANBL","channelID":"100","audienceID":"25d3a628-800a-4e3c-b0e7-bdac1a3698fe","timestamp":"Wed May 14 14:37:05 SAST 2025","sourceSystem":"CARD","product":"CASA","jobName":"SMM815"},"metadata":{"totalFilesProcessed":2,"processingStatus":"Success","eventOutcomeCode":"Success","eventOutcomeDescription":"All customer PDFs processed successfully"},"payload":{"uniqueConsumerRef":"75975a28-e1af-4445-a470-e2df35e1532e","uniqueECPBatchRef":"5c53250c-f0b4-44dd-a1db-7e5f3511c496","filenetObjectID":["C044A38E-0000-C21B-B1E2-69FEE895A17B","D8EFC5A4-0000-B22A-B3D5-74FEE895A17B"],"repositoryID":"Legacy","runPriority":"High","eventID":"E12345","eventType":"Completion","restartKey":"Key12345"},"processedFiles":[{"customerID":null,"pdfFileURL":null}],"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json","timestamp":"Wed May 14 14:37:05 SAST 2025"}
2025-05-14T14:37:05.696+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Returning summary response: {batchID=12345, header={tenantCode=ZANBL, channelID=100, audienceID=25d3a628-800a-4e3c-b0e7-bdac1a3698fe, timestamp=Wed May 14 14:37:05 SAST 2025, sourceSystem=CARD, product=CASA, jobName=SMM815}, metadata={totalFilesProcessed=2, processingStatus=Success, eventOutcomeCode=Success, eventOutcomeDescription=All customer PDFs processed successfully}, payload={uniqueConsumerRef=75975a28-e1af-4445-a470-e2df35e1532e, uniqueECPBatchRef=5c53250c-f0b4-44dd-a1db-7e5f3511c496, filenetObjectID=[C044A38E-0000-C21B-B1E2-69FEE895A17B, D8EFC5A4-0000-B22A-B3D5-74FEE895A17B], repositoryID=Legacy, runPriority=High, eventID=E12345, eventType=Completion, restartKey=Key12345}, processedFiles=[{customerID=null, pdfFileURL=null}], summaryFileURL=https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json, timestamp=Wed May 14 14:37:05 SAST 2025}
2025-05-14T14:37:05.696+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@3e590344, headers={id=daa36397-8a9b-472c-4370-bb3bb3a60c48, timestamp=1747226220378}]]
2025-05-14T14:37:05.696+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T14:37:05.696+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747084602.155617100,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-14T14:37:05.700+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Parsed batchId: 12345, filePath: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv, objectId: {1037A096-0000-CE1A-A484-3290CA7938C2}
2025-05-14T14:37:05.831+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ File uploaded successfully from 'DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv'
2025-05-14T14:37:05.832+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 🔐 SAS URL (valid for 24 hours): https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.832+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : File uploaded to blob storage at URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.839+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Summary published to Kafka topic: str-ecp-batch-composition-complete with message: {"batchID":"12345","header":{"tenantCode":"ZANBL","channelID":"100","audienceID":"aee89ee2-2560-4e2f-a6d3-887f639cf336","timestamp":"Wed May 14 14:37:05 SAST 2025","sourceSystem":"CARD","product":"CASA","jobName":"SMM815"},"metadata":{"totalFilesProcessed":2,"processingStatus":"Success","eventOutcomeCode":"Success","eventOutcomeDescription":"All customer PDFs processed successfully"},"payload":{"uniqueConsumerRef":"2057d308-dca9-4252-b37c-a43d99c8405c","uniqueECPBatchRef":"60d94170-f113-4676-822a-ae42c0e83e27","filenetObjectID":["C044A38E-0000-C21B-B1E2-69FEE895A17B","D8EFC5A4-0000-B22A-B3D5-74FEE895A17B"],"repositoryID":"Legacy","runPriority":"High","eventID":"E12345","eventType":"Completion","restartKey":"Key12345"},"processedFiles":[{"customerID":null,"pdfFileURL":null}],"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json","timestamp":"Wed May 14 14:37:05 SAST 2025"}
2025-05-14T14:37:05.840+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Returning summary response: {batchID=12345, header={tenantCode=ZANBL, channelID=100, audienceID=aee89ee2-2560-4e2f-a6d3-887f639cf336, timestamp=Wed May 14 14:37:05 SAST 2025, sourceSystem=CARD, product=CASA, jobName=SMM815}, metadata={totalFilesProcessed=2, processingStatus=Success, eventOutcomeCode=Success, eventOutcomeDescription=All customer PDFs processed successfully}, payload={uniqueConsumerRef=2057d308-dca9-4252-b37c-a43d99c8405c, uniqueECPBatchRef=60d94170-f113-4676-822a-ae42c0e83e27, filenetObjectID=[C044A38E-0000-C21B-B1E2-69FEE895A17B, D8EFC5A4-0000-B22A-B3D5-74FEE895A17B], repositoryID=Legacy, runPriority=High, eventID=E12345, eventType=Completion, restartKey=Key12345}, processedFiles=[{customerID=null, pdfFileURL=null}], summaryFileURL=https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json, timestamp=Wed May 14 14:37:05 SAST 2025}
2025-05-14T14:37:05.840+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@3e590344, headers={id=daa36397-8a9b-472c-4370-bb3bb3a60c48, timestamp=1747226220378}]]
2025-05-14T14:37:05.840+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T14:37:05.840+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747084602.306001500,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-14T14:37:05.841+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Parsed batchId: 12345, filePath: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv, objectId: {1037A096-0000-CE1A-A484-3290CA7938C2}
2025-05-14T14:37:05.962+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ File uploaded successfully from 'DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv'
2025-05-14T14:37:05.963+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 🔐 SAS URL (valid for 24 hours): https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.964+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : File uploaded to blob storage at URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D
2025-05-14T14:37:05.986+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Summary published to Kafka topic: str-ecp-batch-composition-complete with message: {"batchID":"12345","header":{"tenantCode":"ZANBL","channelID":"100","audienceID":"b044bc81-9809-4a8c-8f2a-0dbf0029b24c","timestamp":"Wed May 14 14:37:05 SAST 2025","sourceSystem":"CARD","product":"CASA","jobName":"SMM815"},"metadata":{"totalFilesProcessed":2,"processingStatus":"Success","eventOutcomeCode":"Success","eventOutcomeDescription":"All customer PDFs processed successfully"},"payload":{"uniqueConsumerRef":"89ecf842-5872-4df0-aaad-c153044eefda","uniqueECPBatchRef":"44eca4a4-505c-413c-a5ab-3d8fc52c543b","filenetObjectID":["C044A38E-0000-C21B-B1E2-69FEE895A17B","D8EFC5A4-0000-B22A-B3D5-74FEE895A17B"],"repositoryID":"Legacy","runPriority":"High","eventID":"E12345","eventType":"Completion","restartKey":"Key12345"},"processedFiles":[{"customerID":null,"pdfFileURL":null}],"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json","timestamp":"Wed May 14 14:37:05 SAST 2025"}
2025-05-14T14:37:05.987+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Returning summary response: {batchID=12345, header={tenantCode=ZANBL, channelID=100, audienceID=b044bc81-9809-4a8c-8f2a-0dbf0029b24c, timestamp=Wed May 14 14:37:05 SAST 2025, sourceSystem=CARD, product=CASA, jobName=SMM815}, metadata={totalFilesProcessed=2, processingStatus=Success, eventOutcomeCode=Success, eventOutcomeDescription=All customer PDFs processed successfully}, payload={uniqueConsumerRef=89ecf842-5872-4df0-aaad-c153044eefda, uniqueECPBatchRef=44eca4a4-505c-413c-a5ab-3d8fc52c543b, filenetObjectID=[C044A38E-0000-C21B-B1E2-69FEE895A17B, D8EFC5A4-0000-B22A-B3D5-74FEE895A17B], repositoryID=Legacy, runPriority=High, eventID=E12345, eventType=Completion, restartKey=Key12345}, processedFiles=[{customerID=null, pdfFileURL=null}], summaryFileURL=https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A05Z&sr=b&sp=r&sig=exJQFjy8mofX%2BKUSE1dddKAF32BaPJZp8gjyALuyFFM%3D/summary/12345_summary.json, timestamp=Wed May 14 14:37:05 SAST 2025}
2025-05-14T14:37:05.988+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@3e590344, headers={id=daa36397-8a9b-472c-4370-bb3bb3a60c48, timestamp=1747226220378}]]
2025-05-14T14:37:05.988+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T14:37:05.988+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747084608.797663900,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-14T14:37:05.989+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Parsed batchId: 12345, filePath: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv, objectId: {1037A096-0000-CE1A-A484-3290CA7938C2}
2025-05-14T14:37:06.101+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : ✅ File uploaded successfully from 'DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv'
2025-05-14T14:37:06.102+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.BlobStorageService       : 🔐 SAS URL (valid for 24 hours): https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A06Z&sr=b&sp=r&sig=EUGIIv%2FHL27pln3cRKlajmRTUDdbtoT0JgFxIDWHHJw%3D
2025-05-14T14:37:06.102+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : File uploaded to blob storage at URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A06Z&sr=b&sp=r&sig=EUGIIv%2FHL27pln3cRKlajmRTUDdbtoT0JgFxIDWHHJw%3D
2025-05-14T14:37:06.108+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Summary published to Kafka topic: str-ecp-batch-composition-complete with message: {"batchID":"12345","header":{"tenantCode":"ZANBL","channelID":"100","audienceID":"3f3a07f0-010d-4ea5-9eed-eda567a037f6","timestamp":"Wed May 14 14:37:06 SAST 2025","sourceSystem":"CARD","product":"CASA","jobName":"SMM815"},"metadata":{"totalFilesProcessed":2,"processingStatus":"Success","eventOutcomeCode":"Success","eventOutcomeDescription":"All customer PDFs processed successfully"},"payload":{"uniqueConsumerRef":"8f041930-aea5-4885-bfe3-9d2a09ddb54a","uniqueECPBatchRef":"53ddf4a2-c84a-425f-a082-91b1917f40b8","filenetObjectID":["C044A38E-0000-C21B-B1E2-69FEE895A17B","D8EFC5A4-0000-B22A-B3D5-74FEE895A17B"],"repositoryID":"Legacy","runPriority":"High","eventID":"E12345","eventType":"Completion","restartKey":"Key12345"},"processedFiles":[{"customerID":null,"pdfFileURL":null}],"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A06Z&sr=b&sp=r&sig=EUGIIv%2FHL27pln3cRKlajmRTUDdbtoT0JgFxIDWHHJw%3D/summary/12345_summary.json","timestamp":"Wed May 14 14:37:06 SAST 2025"}
2025-05-14T14:37:06.109+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Returning summary response: {batchID=12345, header={tenantCode=ZANBL, channelID=100, audienceID=3f3a07f0-010d-4ea5-9eed-eda567a037f6, timestamp=Wed May 14 14:37:06 SAST 2025, sourceSystem=CARD, product=CASA, jobName=SMM815}, metadata={totalFilesProcessed=2, processingStatus=Success, eventOutcomeCode=Success, eventOutcomeDescription=All customer PDFs processed successfully}, payload={uniqueConsumerRef=8f041930-aea5-4885-bfe3-9d2a09ddb54a, uniqueECPBatchRef=53ddf4a2-c84a-425f-a082-91b1917f40b8, filenetObjectID=[C044A38E-0000-C21B-B1E2-69FEE895A17B, D8EFC5A4-0000-B22A-B3D5-74FEE895A17B], repositoryID=Legacy, runPriority=High, eventID=E12345, eventType=Completion, restartKey=Key12345}, processedFiles=[{customerID=null, pdfFileURL=null}], summaryFileURL=https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/1037A096-0000-CE1A-A484-3290CA7938C2_12345.csv?sv=2020-04-08&se=2025-05-15T12%3A37%3A06Z&sr=b&sp=r&sig=EUGIIv%2FHL27pln3cRKlajmRTUDdbtoT0JgFxIDWHHJw%3D/summary/12345_summary.json, timestamp=Wed May 14 14:37:06 SAST 2025}
2025-05-14T14:37:06.109+02:00 DEBUG 5856 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@3e590344, headers={id=daa36397-8a9b-472c-4370-bb3bb3a60c48, timestamp=1747226220378}]]
2025-05-14T14:37:06.109+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T14:37:06.109+02:00  INFO 5856 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
