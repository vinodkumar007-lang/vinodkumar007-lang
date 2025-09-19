2025-09-19T06:27:14.216+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ⬇️ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Downloaded file: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/NDDSSTBR_250902.DAT to /mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSSTBR_250902.DAT
2025-09-19T06:27:16.276+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ⬇️ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Downloaded file: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/NDDSST_250902.DAT to /mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSST_250902.DAT
2025-09-19T06:27:16.279+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=19384, leaderEpoch=null, metadata=''}}
2025-09-19T06:27:16.406+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
	acks = -1
	batch.size = 16384
	bootstrap.servers = [nbpigelpdev02.africa.nedcor.net:9093, nbpproelpdev01.africa.nedcor.net:9093, nbpinelpdev01.africa.nedcor.net:9093]
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
	max.block.ms = 180000
	max.in.flight.requests.per.connection = 5
	max.request.size = 1048576
	metadata.max.age.ms = 30000
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
	request.timeout.ms = 60000
	retries = 5
	retry.backoff.ms = 5000
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
	ssl.keystore.location = /app/keystore/keystore.jks
	ssl.keystore.password = [hidden]
	ssl.keystore.type = JKS
	ssl.protocol = TLSv1.2
	ssl.provider = null
	ssl.secure.random.implementation = null
	ssl.trustmanager.algorithm = PKIX
	ssl.truststore.certificates = null
	ssl.truststore.location = /app/keystore/truststore.jks
	ssl.truststore.password = [hidden]
	ssl.truststore.type = JKS
	transaction.timeout.ms = 60000
	transactional.id = null
	value.serializer = class org.apache.kafka.common.serialization.StringSerializer

2025-09-19T06:27:16.508+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-09-19T06:27:17.005+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-09-19T06:27:17.006+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-09-19T06:27:17.006+02:00  INFO 1 --- [ntainer#0-0-C-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1758256037005
2025-09-19T06:27:17.009+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@213c552e]
2025-09-19T06:27:17.536+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:17.699+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:22.537+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:22.700+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:25.839+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -3 due to socket connection setup timeout. The timeout value is 8685 ms.
2025-09-19T06:27:25.840+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpinelpdev01.africa.nedcor.net:9093 (id: -3 rack: null) disconnected
2025-09-19T06:27:27.538+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:27.701+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:30.841+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 9552 ms.
2025-09-19T06:27:30.841+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpproelpdev01.africa.nedcor.net:9093 (id: -2 rack: null) disconnected
2025-09-19T06:27:32.540+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:32.701+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:37.540+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:37.701+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:42.541+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:42.701+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:47.543+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:47.703+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:47.790+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -1 due to socket connection setup timeout. The timeout value is 11856 ms.
2025-09-19T06:27:47.790+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpigelpdev02.africa.nedcor.net:9093 (id: -1 rack: null) disconnected
2025-09-19T06:27:52.544+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:52.703+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:57.544+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:27:57.703+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:02.544+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:02.703+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:07.544+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:07.703+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:11.776+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -3 due to socket connection setup timeout. The timeout value is 18943 ms.
2025-09-19T06:28:11.777+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpinelpdev01.africa.nedcor.net:9093 (id: -3 rack: null) disconnected
2025-09-19T06:28:12.545+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:12.705+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:17.545+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:17.705+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:22.547+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:22.706+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:27.547+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:27.707+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:32.548+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:32.708+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:37.548+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:37.708+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:39.457+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 22647 ms.
2025-09-19T06:28:39.458+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpproelpdev01.africa.nedcor.net:9093 (id: -2 rack: null) disconnected
2025-09-19T06:28:42.549+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:42.709+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:47.550+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:47.710+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:52.551+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:52.710+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:57.552+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:28:57.711+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:00.982+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -1 due to socket connection setup timeout. The timeout value is 16504 ms.
2025-09-19T06:29:00.983+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpigelpdev02.africa.nedcor.net:9093 (id: -1 rack: null) disconnected
2025-09-19T06:29:02.553+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:02.712+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:07.554+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:07.714+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:12.554+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:12.714+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:17.555+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:17.714+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:22.556+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:22.716+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:27.556+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:27.716+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:32.556+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:32.716+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:33.749+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -3 due to socket connection setup timeout. The timeout value is 27733 ms.
2025-09-19T06:29:33.750+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpinelpdev01.africa.nedcor.net:9093 (id: -3 rack: null) disconnected
2025-09-19T06:29:37.557+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:37.717+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:42.558+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:42.718+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:47.558+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:47.719+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:52.559+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:52.719+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:57.560+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:29:57.720+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:02.561+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:02.721+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:07.561+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:07.722+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:10.978+02:00  INFO 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Disconnecting from node -2 due to socket connection setup timeout. The timeout value is 32227 ms.
2025-09-19T06:30:10.978+02:00  WARN 1 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker nbpproelpdev01.africa.nedcor.net:9093 (id: -2 rack: null) disconnected
2025-09-19T06:30:12.561+02:00 DEBUG 1 --- [ntainer#0-2-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:12.722+02:00 DEBUG 1 --- [ntainer#0-1-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-09-19T06:30:17.208+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='f6a1bdb5-2fb2-4f77-a7d3-7087758a2572' and payload='{"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourc...' to topic log-ecp-batch-audit:

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-19T06:30:17.213+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(topic=log-ecp-batch-audit, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = X-dynaTrace, value = [0, 0, 0, 4, -36, -63, 104, -99, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 2, -63, 28, 91, 39, 20, 117, -120, -111, 0, 0, 1, 68, -58, 51, 4, 8, 0, 0, 0, 1, 6, 8, -36, -63, 104, -99, 8, 8, 0, 0, 0, 0, 10, 8, 0, 0, 0, 1, 12, 32, 25, 102, -52, 87, -114, -91, 117, -48, -3, -102, -112, -53, 110, -20, -125, 91, 14, 16, -28, 84, 107, -52, -10, -15, 26, -59]), RecordHeader(key = traceparent, value = [48, 48, 45, 49, 57, 54, 54, 99, 99, 53, 55, 56, 101, 97, 53, 55, 53, 100, 48, 102, 100, 57, 97, 57, 48, 99, 98, 54, 101, 101, 99, 56, 51, 53, 98, 45, 101, 52, 53, 52, 54, 98, 99, 99, 102, 54, 102, 49, 49, 97, 99, 53, 45, 48, 49]), RecordHeader(key = tracestate, value = [49, 52, 55, 53, 56, 56, 57, 49, 45, 99, 49, 49, 99, 53, 98, 50, 55, 64, 100, 116, 61, 102, 119, 52, 59, 50, 59, 100, 99, 99, 49, 54, 56, 57, 100, 59, 48, 59, 99, 59, 48, 59, 48, 59, 49, 52, 52, 59, 53, 101, 98, 51, 59, 50, 104, 48, 49, 59, 51, 104, 100, 99, 99, 49, 54, 56, 57, 100, 59, 52, 104, 48, 48, 59, 53, 104, 48, 49, 59, 54, 104, 49, 57, 54, 54, 99, 99, 53, 55, 56, 101, 97, 53, 55, 53, 100, 48, 102, 100, 57, 97, 57, 48, 99, 98, 54, 101, 101, 99, 56, 51, 53, 98, 59, 55, 104, 101, 52, 53, 52, 54, 98, 99, 99, 102, 54, 102, 49, 49, 97, 99, 53])], isReadOnly = false), key=f6a1bdb5-2fb2-4f77-a7d3-7087758a2572, value={"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourceSystem":"MFC","tenantCode":"ZANBL","channelID":"<value>","product":"MFC","jobName":"MFC","uniqueConsumerRef":"19ef9d68-b114-4803-b09b-95a6c5fa4644","timestamp":"2025-09-19T04:27:16.283499558Z","runPriority":null,"eventType":"INBOUND","startTime":"2025-09-19T04:27:16.283306352Z","endTime":"2025-09-19T04:27:16.283306352Z","customerCount":2,"batchFiles":[{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSSTBR_250902.DAT","fileName":"NDDSSTBR_250902.DAT","fileType":"REF"},{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSST_250902.DAT","fileName":"NDDSST_250902.DAT","fileType":"DATA"}]}, timestamp=null)

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-19T06:30:17.214+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Kafka message processing failed. Error: Send failed

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:544) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.sendToAuditTopic(KafkaListenerService.java:934) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.onKafkaMessage(KafkaListenerService.java:192) ~[classes!/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.invoke(InvocableHandlerMethod.java:119) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.kafka.listener.adapter.HandlerAdapter.invoke(HandlerAdapter.java:56) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:375) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeOnMessage(KafkaMessageListenerContainer.java:2854) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.lambda$doInvokeRecordListener$57(KafkaMessageListenerContainer.java:2772) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at io.micrometer.observation.Observation.observe(Observation.java:559) ~[micrometer-observation-1.10.2.jar!/:1.10.2]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeRecordListener(KafkaMessageListenerContainer.java:2770) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeWithRecords(KafkaMessageListenerContainer.java:2622) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeRecordListener(KafkaMessageListenerContainer.java:2508) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeListener(KafkaMessageListenerContainer.java:2150) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeIfHaveRecords(KafkaMessageListenerContainer.java:1505) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.pollAndInvoke(KafkaMessageListenerContainer.java:1469) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.run(KafkaMessageListenerContainer.java:1344) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms
