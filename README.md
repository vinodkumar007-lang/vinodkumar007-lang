Caused by: org.apache.http.conn.HttpHostConnectException: Connect to vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200 [vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud/40.68.97.112] failed: Connection timed out: connect
	at org.apache.http.impl.conn.DefaultHttpClientConnectionOperator.connect(DefaultHttpClientConnectionOperator.java:156) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.conn.PoolingHttpClientConnectionManager.connect(PoolingHttpClientConnectionManager.java:376) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.execchain.MainClientExec.establishRoute(MainClientExec.java:393) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.execchain.MainClientExec.execute(MainClientExec.java:236) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:186) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.execchain.RetryExec.execute(RetryExec.java:89) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.execchain.RedirectExec.execute(RedirectExec.java:110) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:185) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:83) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.client.CloseableHttpClient.execute(CloseableHttpClient.java:108) ~[httpclient-4.5.13.jar:4.5.13]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.getVaultToken(BlobStorageService.java:104) ~[classes/:na]
	... 26 common frames omitted
Caused by: java.net.ConnectException: Connection timed out: connect
	at java.base/sun.nio.ch.Net.connect0(Native Method) ~[na:na]
	at java.base/sun.nio.ch.Net.connect(Net.java:579) ~[na:na]
	at java.base/sun.nio.ch.Net.connect(Net.java:568) ~[na:na]
	at java.base/sun.nio.ch.NioSocketImpl.connect(NioSocketImpl.java:593) ~[na:na]
	at java.base/java.net.SocksSocketImpl.connect(SocksSocketImpl.java:327) ~[na:na]
	at java.base/java.net.Socket.connect(Socket.java:633) ~[na:na]
	at org.apache.http.conn.ssl.SSLConnectionSocketFactory.connectSocket(SSLConnectionSocketFactory.java:368) ~[httpclient-4.5.13.jar:4.5.13]
	at org.apache.http.impl.conn.DefaultHttpClientConnectionOperator.connect(DefaultHttpClientConnectionOperator.java:142) ~[httpclient-4.5.13.jar:4.5.13]
	... 36 common frames omitted
