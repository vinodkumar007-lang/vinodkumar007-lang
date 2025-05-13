06:01:35.081 [main] DEBUG org.apache.http.client.protocol.RequestAuthCache - Auth cache not set in the context
06:01:35.085 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection request: [route: {s}->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 0 of 2; total allocated: 0 of 20]
06:01:35.105 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection leased: [id: 0][route: {s}->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 1 of 2; total allocated: 1 of 20]
06:01:35.107 [main] DEBUG org.apache.http.impl.execchain.MainClientExec - Opening connection {s}->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
06:01:35.144 [main] DEBUG org.apache.http.impl.conn.DefaultHttpClientConnectionOperator - Connecting to vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud/40.68.97.112:8200
06:01:35.145 [main] DEBUG org.apache.http.conn.ssl.SSLConnectionSocketFactory - Connecting socket to vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud/40.68.97.112:8200 with timeout 20000
06:01:55.155 [main] DEBUG org.apache.http.impl.conn.DefaultManagedHttpClientConnection - http-outgoing-0: Shutdown connection
06:01:55.155 [main] DEBUG org.apache.http.impl.execchain.MainClientExec - Connection discarded
06:01:55.156 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection released: [id: 0][route: {s}->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 0 of 2; total allocated: 0 of 20]
❌ Error during connection:
org.apache.http.conn.ConnectTimeoutException: Connect to vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200 [vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud/40.68.97.112] failed: Connect timed out
	at org.apache.http.impl.conn.DefaultHttpClientConnectionOperator.connect(DefaultHttpClientConnectionOperator.java:151)
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager.connect(PoolingHttpClientConnectionManager.java:376)
	at org.apache.http.impl.execchain.MainClientExec.establishRoute(MainClientExec.java:393)
	at org.apache.http.impl.execchain.MainClientExec.execute(MainClientExec.java:236)
	at org.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:186)
	at org.apache.http.impl.execchain.RetryExec.execute(RetryExec.java:89)
	at org.apache.http.impl.execchain.RedirectExec.execute(RedirectExec.java:110)
	at org.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:185)
	at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:83)
	at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:108)
	at com.nedbank.kafka.filemanage.service.VaultTester.main(VaultTester.java:60)
Caused by: java.net.SocketTimeoutException: Connect timed out
	at java.base/sun.nio.ch.NioSocketImpl.timedFinishConnect(NioSocketImpl.java:551)
	at java.base/sun.nio.ch.NioSocketImpl.connect(NioSocketImpl.java:602)
	at java.base/java.net.SocksSocketImpl.connect(SocksSocketImpl.java:327)
	at java.base/java.net.Socket.connect(Socket.java:633)
	at org.apache.http.conn.ssl.SSLConnectionSocketFactory.connectSocket(SSLConnectionSocketFactory.java:368)
	at org.apache.http.impl.conn.DefaultHttpClientConnectionOperator.connect(DefaultHttpClientConnectionOperator.java:142)
	... 10 more
