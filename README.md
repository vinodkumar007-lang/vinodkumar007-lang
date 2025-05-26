2025-05-26T10:10:41.103+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T10:10:41.106+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T10:10:41.108+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748247041100
2025-05-26T10:10:42.063+02:00  INFO 17748 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T10:10:42.068+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-05-26T10:10:42.211+02:00  INFO 17748 --- [nio-8080-exec-2] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-26T10:10:42.230+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Seeking to offset 18498 for partition str-ecp-batch-composition-0
2025-05-26T10:10:42.232+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Seeking partition 0 to offset from 10 days ago: 18498
2025-05-26T10:10:42.315+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Polled 26 record(s) from Kafka
2025-05-26T10:10:42.316+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Processing record from topic-partition-offset str-ecp-batch-composition-0-18498: key='null'
2025-05-26T10:10:42.336+02:00  WARN 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Missing mandatory field 'BatchId'
2025-05-26T10:10:42.378+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.utils.SummaryJsonWriter          : Appended to summary.json: C:\Users\CC437236\summary.json
2025-05-26T10:10:42.378+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Updated lastProcessedOffsets: {str-ecp-batch-composition-0=18498}
2025-05-26T10:10:42.389+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
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

2025-05-26T10:10:42.402+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-05-26T10:10:42.490+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-26T10:10:42.490+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-26T10:10:42.490+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1748247042490
2025-05-26T10:10:42.493+02:00 DEBUG 17748 --- [nio-8080-exec-2] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@655fbe7d]
2025-05-26T10:10:42.586+02:00  INFO 17748 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition str-ecp-batch-composition-complete-0 to 30 since the associated topicId changed from null to kFgHvXbjT6KB5Z2coZAcJw
2025-05-26T10:10:42.586+02:00  INFO 17748 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-26T10:10:42.590+02:00  INFO 17748 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 195067 with epoch 0
2025-05-26T10:10:42.602+02:00  INFO 17748 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Final Response sent to topic: str-ecp-batch-composition-complete
2025-05-26T10:10:42.824+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-05-26T10:10:42.881+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting generation and member id due to: consumer pro-actively leaving the group
2025-05-26T10:10:42.881+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-05-26T10:10:42.881+02:00  INFO 17748 --- [nio-8080-exec-2] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-05-26T10:10:42.881+02:00  INFO 17748 --- [nio-8080-exec-2] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-05-26T10:10:42.882+02:00  INFO 17748 --- [nio-8080-exec-2] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-05-26T10:10:42.889+02:00  INFO 17748 --- [nio-8080-exec-2] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for consumer-str-ecp-batch-1 unregistered
