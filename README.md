2025-07-10T13:58:38.841+02:00  WARN 1 --- [ntainer#0-0-C-1] org.apache.kafka.clients.NetworkClient   : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Bootstrap broker nsnxeteelpka01.nednet.co.za:9093 (id: -1 rack: null) disconnected
2025-07-10T13:58:38.845+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Authentication/Authorization Exception and no authExceptionRetryInterval set

org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed
Caused by: javax.net.ssl.SSLHandshakeException: Unknown identification algorithm: true;
	at java.base/sun.security.ssl.Alert.createSSLException(Alert.java:131) ~[na:na]
	at java.base/sun.security.ssl.TransportContext.fatal(TransportContext.java:378) ~[na:na]
	at java.base/sun.security.ssl.TransportContext.fatal(TransportContext.java:321) ~[na:na]
	at java.base/sun.security.ssl.TransportContext.fatal(TransportContext.java:316) ~[na:na]
	at java.base/sun.security.ssl.CertificateMessage$T12CertificateConsumer.checkServerCerts(CertificateMessage.java:654) ~[na:na]
	at java.base/sun.security.ssl.CertificateMessage$T12CertificateConsumer.onCertificate(CertificateMessage.java:473) ~[na:na]
	at java.base/sun.security.ssl.CertificateMessage$T12CertificateConsumer.consume(CertificateMessage.java:369) ~[na:na]
	at java.base/sun.security.ssl.SSLHandshake.consume(SSLHandshake.java:396) ~[na:na]
	at java.base/sun.security.ssl.HandshakeContext.dispatch(HandshakeContext.java:480) ~[na:na]
	at java.base/sun.security.ssl.SSLEngineImpl$DelegatedTask$DelegatedAction.run(SSLEngineImpl.java:1277) ~[na:na]
