2025-06-09T07:08:13.408+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.c.c.C.[Tomcat].[localhost].[/]       : Initializing Spring DispatcherServlet 'dispatcherServlet'
2025-06-09T07:08:13.409+02:00  INFO 19640 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Initializing Servlet 'dispatcherServlet'
2025-06-09T07:08:13.411+02:00  INFO 19640 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Completed initialization in 1 ms
2025-06-09T07:08:13.459+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.c.FileProcessingController       : POST /process called to trigger Kafka message processing.
2025-06-09T07:08:13.460+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.clients.consumer.ConsumerConfig    : ConsumerConfig values: 
	allow.auto.create.topics = true
	auto.commit.interval.ms = 5000
	auto.offset.reset = earliest
	bootstrap.servers = [nsnxeteelpka01.nednet.co.za:9093, nsnxeteelpka02.nednet.co.za:9093, nsnxeteelpka03.nednet.co.za:9093]
	check.crcs = true
	client.dns.lookup = use_all_dns_ips
	client.id = consumer-str-ecp-batch-2
	client.rack = 
	connections.max.idle.ms = 540000
	default.api.timeout.ms = 60000
	enable.auto.commit = false
	exclude.internal.topics = true
	fetch.max.bytes = 52428800
	fetch.max.wait.ms = 500
	fetch.min.bytes = 1
	group.id = str-ecp-batch
	group.instance.id = null
	heartbeat.interval.ms = 3000
	interceptor.classes = []
	internal.leave.group.on.close = true
	internal.throw.on.fetch.stable.offset.unsupported = false
	isolation.level = read_uncommitted
	key.deserializer = class org.apache.kafka.common.serialization.StringDeserializer
	max.partition.fetch.bytes = 1048576
	max.poll.interval.ms = 300000
	max.poll.records = 500
	metadata.max.age.ms = 300000
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
	receive.buffer.bytes = 65536
	reconnect.backoff.max.ms = 1000
	reconnect.backoff.ms = 50
	request.timeout.ms = 30000
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
	session.timeout.ms = 45000
	socket.connection.setup.timeout.max.ms = 30000
	socket.connection.setup.timeout.ms = 10000
	ssl.cipher.suites = null
	ssl.enabled.protocols = [TLSv1.2, TLSv1.3]
	ssl.endpoint.identification.algorithm = 
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
	value.deserializer = class org.apache.kafka.common.serialization.StringDeserializer

2025-06-09T07:08:13.515+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-06-09T07:08:13.515+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-06-09T07:08:13.515+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1749445693515
2025-06-09T07:08:13.516+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Assigned to partition(s): str-ecp-batch-composition-0
2025-06-09T07:08:14.162+02:00  INFO 19640 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-06-09T07:08:14.166+02:00  INFO 19640 --- [nio-8080-exec-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-06-09T07:08:14.168+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-06-09T07:08:14.251+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Seeking to offset 18606 for partition str-ecp-batch-composition-0
2025-06-09T07:08:14.480+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : 🔐 Fetching secrets from Azure Key Vault...
2025-06-09T07:08:14.543+02:00 ERROR 19640 --- [nio-8080-exec-1] c.azure.identity.EnvironmentCredential   : Azure Identity => ERROR in EnvironmentCredential: Missing required environment variable AZURE_CLIENT_ID
2025-06-09T07:08:14.725+02:00 ERROR 19640 --- [nio-8080-exec-1] c.a.i.i.IntelliJCacheAccessor            : IntelliJ Authentication not available. Please log in with Azure Tools for IntelliJ plugin in the IDE.
2025-06-09T07:08:14.987+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : Vault secrets initialized for Blob Storage
2025-06-09T07:08:14.987+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Secrets fetched successfully from Azure Key Vault.
2025-06-09T07:08:15.721+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110543680509, accountNumber=3768000010607501, name=null, email=446.83, mobileNumber=6000.00, deliveryChannel=EMAIL, addressLine1=00912 SULA, addressLine2=MANCHESTER ROAD, addressLine3=MANABA BEACH MANABA BEACH, postalCode=+27999999999, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=20.65000, creditLimit=null, interestRate=null, dueAmount=40.00, arrears=null, dueDate=null, idNumber=2017783, accountReference=null, contactEmail=null, contactDepartment=null, firstName=ASHWIN BADRI, lastName=DAY, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=CreditCard, fullName=ASHWIN BADRI DAY)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110067975607, accountNumber=5898460759264500, name=null, email=526.61, mobileNumber=1500.00, deliveryChannel=EMAIL, addressLine1=62 ZELDA COURTS, addressLine2=WINDSOR MINE, addressLine3=KRUGERSDORP, postalCode=+27997975607, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=11.00000, creditLimit=null, interestRate=null, dueAmount=526.61, arrears=null, dueDate=null, idNumber=1063804, accountReference=null, contactEmail=null, contactDepartment=null, firstName=JAMES, lastName=WHITE, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=CreditCard, fullName=JAMES WHITE)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110051703106, accountNumber=5898460773139035, name=null, email=3081.29, mobileNumber=900.00, deliveryChannel=EMAIL, addressLine1=135 RIVONIA ROAD, addressLine2=, addressLine3=SANDOWN, postalCode=+27995111237, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=16.00000, creditLimit=null, interestRate=null, dueAmount=0.00, arrears=null, dueDate=null, idNumber=1534830, accountReference=null, contactEmail=null, contactDepartment=null, firstName=DEVERAUX WESLEY, lastName=NKOSI, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=CreditCard, fullName=DEVERAUX WESLEY NKOSI)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110568881702, accountNumber=5898460773863931, name=null, email=528.21, mobileNumber=2000.00, deliveryChannel=MOBSTAT, addressLine1=UNIT 9, addressLine2=PASTORIE PARK REITZ STREET, addressLine3=SOMERSET WEST, postalCode=+27737392815, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=19.65000, creditLimit=null, interestRate=null, dueAmount=40.00, arrears=null, dueDate=null, idNumber=1985369, accountReference=null, contactEmail=null, contactDepartment=null, firstName=K  KATHERINE JEAN, lastName=MCLACHLAN, tags=null, printIndicator=null, tenantCode=null, channel=MOBSTAT, language=Eng, currency=ZAR, productCode=CreditCard, fullName=K  KATHERINE JEAN MCLACHLAN)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110051703106, accountNumber=5898460773874268, name=null, email=631.28, mobileNumber=2000.00, deliveryChannel=EMAIL, addressLine1=135 RIVONIA ROAD, addressLine2=, addressLine3=SANDOWN, postalCode=+27995111237, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=10.00000, creditLimit=null, interestRate=null, dueAmount=60.00, arrears=null, dueDate=null, idNumber=2005556, accountReference=null, contactEmail=null, contactDepartment=null, firstName=DEVERAUX WESLEY, lastName=NKOSI, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=CreditCard, fullName=DEVERAUX WESLEY NKOSI)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=600002818430, accountNumber=8000493779301, name=null, email=, mobileNumber=, deliveryChannel=PRINT, addressLine1=11 BERRY STREET, addressLine2=WELTEVREDEN PARK X, addressLine3=WILROPARK EXT 1, postalCode=+27991299538, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1727593, accountReference=null, contactEmail=null, contactDepartment=null, firstName=LOUELLA TWO, lastName=GILLECE, tags=null, printIndicator=null, tenantCode=null, channel=PRINT, language=Eng, currency=ZAR, productCode=HomeLoan, fullName=LOUELLA TWO GILLECE)
2025-06-09T07:08:15.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110001034103, accountNumber=8001086778201, name=null, email=, mobileNumber=, deliveryChannel=EMAIL, addressLine1=ROSMEADLAAN 2, addressLine2=, addressLine3=ORANJEZICHT, postalCode=+27991034103, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1734855, accountReference=null, contactEmail=null, contactDepartment=null, firstName=ELGONDA ERITZEMA, lastName=STRANGFELD, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=HomeLoan, fullName=ELGONDA ERITZEMA STRANGFELD)
2025-06-09T07:08:15.749+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=191103367327, accountNumber=8001449756501, name=null, email=, mobileNumber=, deliveryChannel=EMAIL, addressLine1=VAN DER POST SINGEL 10, addressLine2=, addressLine3=FICHARDTPARK, postalCode=+27833902010, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1943677, accountReference=null, contactEmail=null, contactDepartment=null, firstName=FRANCOIS JAKOBUS, lastName=RETIEF, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=HomeLoan, fullName=FRANCOIS JAKOBUS RETIEF)
2025-06-09T07:08:15.749+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=600002820705, accountNumber=8002305505501, name=null, email=, mobileNumber=, deliveryChannel=PRINT, addressLine1=8 LARSON STREET, addressLine2=WELTEVREDEN PARK X, addressLine3=WILROPARK EXT 1, postalCode=+27991297411, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1996426, accountReference=null, contactEmail=null, contactDepartment=null, firstName=PERCY TWO, lastName=MCGLAUN, tags=null, printIndicator=null, tenantCode=null, channel=PRINT, language=Eng, currency=ZAR, productCode=HomeLoan, fullName=PERCY TWO MCGLAUN)
2025-06-09T07:08:15.749+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=110596232802, accountNumber=8002656651401, name=null, email=, mobileNumber=, deliveryChannel=PRINT, addressLine1=83 SUNBIRD RYLAAN, addressLine2=, addressLine3=LANGEBAAN, postalCode=+27991154689, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1719310, accountReference=null, contactEmail=null, contactDepartment=null, firstName=TRONELLE, lastName=STEENEKAMP, tags=null, printIndicator=null, tenantCode=null, channel=PRINT, language=Eng, currency=ZAR, productCode=HomeLoan, fullName=TRONELLE STEENEKAMP)
2025-06-09T07:08:15.749+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.kafka.filemanage.service.DataParser  : Extracted customer: CustomerData(customerId=191850154905, accountNumber=8007035935001, name=null, email=, mobileNumber=, deliveryChannel=EMAIL, addressLine1=90 BRAM FISCHER ROAD, addressLine2=, addressLine3=DURBAN CENTRAL, postalCode=+27810469924, contactNumber=null, product=null, branchCode=null, templateCode=null, templateName=null, balance=, creditLimit=null, interestRate=null, dueAmount=, arrears=null, dueDate=null, idNumber=1631066, accountReference=null, contactEmail=null, contactDepartment=null, firstName=CORLI, lastName=COETSEE, tags=null, printIndicator=null, tenantCode=null, channel=EMAIL, language=Eng, currency=ZAR, productCode=PersonalLoan, fullName=CORLI COETSEE)
2025-06-09T07:08:15.898+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110543680509_1781525962435016220.pdf'
2025-06-09T07:08:15.967+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110543680509_1781525962435016220.pdf'
2025-06-09T07:08:16.039+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110543680509_10719371733830905543.html'
2025-06-09T07:08:16.126+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110543680509_3443698077803839618.txt'
2025-06-09T07:08:16.200+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110543680509_13528893420668009638.mobstat'
2025-06-09T07:08:16.278+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110067975607_15465130359358698611.pdf'
2025-06-09T07:08:16.336+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110067975607_15465130359358698611.pdf'
2025-06-09T07:08:16.419+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110067975607_7499914335362236916.html'
2025-06-09T07:08:16.530+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110067975607_5847541066525912906.txt'
2025-06-09T07:08:16.584+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110067975607_169323133346176719.mobstat'
2025-06-09T07:08:16.650+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110051703106_1194330366231666202.pdf'
2025-06-09T07:08:16.707+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110051703106_1194330366231666202.pdf'
2025-06-09T07:08:16.770+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110051703106_1457061936829114689.html'
2025-06-09T07:08:16.854+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110051703106_17240314563254389519.txt'
2025-06-09T07:08:16.942+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110051703106_14720195068871503732.mobstat'
2025-06-09T07:08:17.005+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110568881702_17130604557377610834.pdf'
2025-06-09T07:08:17.078+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110568881702_17130604557377610834.pdf'
2025-06-09T07:08:17.141+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110568881702_12119241490878046233.html'
2025-06-09T07:08:17.205+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110568881702_16966518878950147975.txt'
2025-06-09T07:08:17.282+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110568881702_12113257853676002419.mobstat'
2025-06-09T07:08:17.359+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110051703106_5601865416721109945.pdf'
2025-06-09T07:08:17.418+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110051703106_5601865416721109945.pdf'
2025-06-09T07:08:17.496+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110051703106_8752004736999376346.html'
2025-06-09T07:08:17.558+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110051703106_12024942174337115772.txt'
2025-06-09T07:08:17.641+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110051703106_3934144214352929052.mobstat'
2025-06-09T07:08:17.748+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F600002818430_13805491546563318885.pdf'
2025-06-09T07:08:17.809+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F600002818430_13805491546563318885.pdf'
2025-06-09T07:08:17.872+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F600002818430_7316116175454177247.html'
2025-06-09T07:08:17.941+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F600002818430_12768416387346394117.txt'
2025-06-09T07:08:18.039+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F600002818430_10728551427202503948.mobstat'
2025-06-09T07:08:18.114+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110001034103_17382746051312327993.pdf'
2025-06-09T07:08:18.183+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110001034103_17382746051312327993.pdf'
2025-06-09T07:08:18.242+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110001034103_5799684851433008384.html'
2025-06-09T07:08:18.304+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110001034103_13819137591304372056.txt'
2025-06-09T07:08:18.374+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110001034103_6597113146143229523.mobstat'
2025-06-09T07:08:18.442+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F191103367327_18252040049409571275.pdf'
2025-06-09T07:08:18.499+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F191103367327_18252040049409571275.pdf'
2025-06-09T07:08:18.553+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F191103367327_1393036350922977932.html'
2025-06-09T07:08:18.614+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F191103367327_11221293771192615366.txt'
2025-06-09T07:08:18.679+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F191103367327_3390680017862453897.mobstat'
2025-06-09T07:08:18.752+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F600002820705_5472703024078141577.pdf'
2025-06-09T07:08:18.831+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F600002820705_5472703024078141577.pdf'
2025-06-09T07:08:18.894+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F600002820705_2243932138242411602.html'
2025-06-09T07:08:18.954+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F600002820705_10139294148871367463.txt'
2025-06-09T07:08:19.015+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F600002820705_17996996106537931083.mobstat'
2025-06-09T07:08:19.099+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F110596232802_457330061109165320.pdf'
2025-06-09T07:08:19.159+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F110596232802_457330061109165320.pdf'
2025-06-09T07:08:19.218+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F110596232802_12161987851244569048.html'
2025-06-09T07:08:19.275+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F110596232802_15689022042967626783.txt'
2025-06-09T07:08:19.347+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F110596232802_3074264188294702216.mobstat'
2025-06-09T07:08:19.423+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Farchive%2F191850154905_6529973338044509318.pdf'
2025-06-09T07:08:19.487+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Femail%2F191850154905_6529973338044509318.pdf'
2025-06-09T07:08:19.560+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fhtml%2F191850154905_3817686225027905999.html'
2025-06-09T07:08:19.628+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Ftxt%2F191850154905_10461087895075255968.txt'
2025-06-09T07:08:19.685+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1970-01-21%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2FDEBTMAN%2Fmobstat%2F191850154905_8923682635343961777.mobstat'
2025-06-09T07:08:19.686+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.KafkaListenerService     : Total processed files count: 11
2025-06-09T07:08:19.710+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.utils.SummaryJsonWriter          : ✅ Summary JSON successfully written to file: C:\Users\CC437236\AppData\Local\Temp\summary-9284560273551426152.json
2025-06-09T07:08:19.802+02:00  INFO 19640 --- [nio-8080-exec-1] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2F1c93525b-42d1-410a-9e26-aa957f19861c%2F6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f%2Fsummary.json'
2025-06-09T07:08:19.818+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.clients.producer.ProducerConfig    : ProducerConfig values: 
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

2025-06-09T07:08:19.828+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.clients.producer.KafkaProducer     : [Producer clientId=producer-1] Instantiated an idempotent producer.
2025-06-09T07:08:19.877+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-06-09T07:08:19.877+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-06-09T07:08:19.877+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1749445699877
2025-06-09T07:08:19.878+02:00 DEBUG 19640 --- [nio-8080-exec-1] o.s.k.core.DefaultKafkaProducerFactory   : Created new Producer: CloseSafeProducer [delegate=org.apache.kafka.clients.producer.KafkaProducer@7bf665b6]
2025-06-09T07:08:19.949+02:00  INFO 19640 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Resetting the last seen epoch of partition str-ecp-batch-composition-complete-0 to 30 since the associated topicId changed from null to kFgHvXbjT6KB5Z2coZAcJw
2025-06-09T07:08:19.950+02:00  INFO 19640 --- [ad | producer-1] org.apache.kafka.clients.Metadata        : [Producer clientId=producer-1] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-06-09T07:08:19.963+02:00  INFO 19640 --- [ad | producer-1] o.a.k.c.p.internals.TransactionManager   : [Producer clientId=producer-1] ProducerId set to 194150 with epoch 0
2025-06-09T07:08:19.967+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Resetting generation and member id due to: consumer pro-actively leaving the group
2025-06-09T07:08:19.967+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-2, groupId=str-ecp-batch] Request joining group due to: consumer pro-actively leaving the group
2025-06-09T07:08:19.967+02:00  INFO 19640 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Metrics scheduler closed
2025-06-09T07:08:19.968+02:00  INFO 19640 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Closing reporter org.apache.kafka.common.metrics.JmxReporter
2025-06-09T07:08:19.968+02:00  INFO 19640 --- [nio-8080-exec-1] o.apache.kafka.common.metrics.Metrics    : Metrics reporters closed
2025-06-09T07:08:19.973+02:00  INFO 19640 --- [nio-8080-exec-1] o.a.kafka.common.utils.AppInfoParser     : App info kafka.consumer for consumer-str-ecp-batch-2 unregistered
