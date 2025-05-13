06:35:26.921 [main] DEBUG org.apache.http.client.protocol.RequestAddCookies - CookieSpec selected: default
06:35:26.937 [main] DEBUG org.apache.http.client.protocol.RequestAuthCache - Auth cache not set in the context
06:35:26.938 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection request: [route: {tls}->http://webproxy.africa.nedcor.net:9001->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 0 of 2; total allocated: 0 of 20]
06:35:26.959 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection leased: [id: 0][route: {tls}->http://webproxy.africa.nedcor.net:9001->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 1 of 2; total allocated: 1 of 20]
06:35:26.961 [main] DEBUG org.apache.http.impl.execchain.MainClientExec - Opening connection {tls}->http://webproxy.africa.nedcor.net:9001->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
06:35:26.965 [main] DEBUG org.apache.http.impl.conn.DefaultHttpClientConnectionOperator - Connecting to webproxy.africa.nedcor.net/10.59.235.139:9001
06:35:26.977 [main] DEBUG org.apache.http.impl.conn.DefaultHttpClientConnectionOperator - Connection established 10.74.132.11:52429<->10.59.235.139:9001
06:35:26.980 [main] DEBUG org.apache.http.headers - http-outgoing-0 >> CONNECT vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200 HTTP/1.1
06:35:26.980 [main] DEBUG org.apache.http.headers - http-outgoing-0 >> Host: vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
06:35:26.980 [main] DEBUG org.apache.http.headers - http-outgoing-0 >> User-Agent: Apache-HttpClient/4.5.13 (Java/17.0.12)
06:35:26.980 [main] DEBUG org.apache.http.wire - http-outgoing-0 >> "CONNECT vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200 HTTP/1.1[\r][\n]"
06:35:26.980 [main] DEBUG org.apache.http.wire - http-outgoing-0 >> "Host: vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200[\r][\n]"
06:35:26.980 [main] DEBUG org.apache.http.wire - http-outgoing-0 >> "User-Agent: Apache-HttpClient/4.5.13 (Java/17.0.12)[\r][\n]"
06:35:26.980 [main] DEBUG org.apache.http.wire - http-outgoing-0 >> "[\r][\n]"
06:35:26.981 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "HTTP/1.1 400 Bad Request[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "Content-Type: text/html; charset=us-ascii[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "Server: Microsoft-HTTPAPI/2.0[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "Date: Tue, 13 May 2025 04:35:26 GMT[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "Connection: close[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "Content-Length: 324[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN""http://www.w3.org/TR/html4/strict.dtd">[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "<HTML><HEAD><TITLE>Bad Request</TITLE>[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "<META HTTP-EQUIV="Content-Type" Content="text/html; charset=us-ascii"></HEAD>[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "<BODY><h2>Bad Request - Invalid URL</h2>[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "<hr><p>HTTP Error 400. The request URL is invalid.</p>[\r][\n]"
06:35:26.982 [main] DEBUG org.apache.http.wire - http-outgoing-0 << "</BODY></HTML>[\r][\n]"
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << HTTP/1.1 400 Bad Request
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << Content-Type: text/html; charset=us-ascii
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << Server: Microsoft-HTTPAPI/2.0
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << Date: Tue, 13 May 2025 04:35:26 GMT
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << Connection: close
06:35:26.986 [main] DEBUG org.apache.http.headers - http-outgoing-0 << Content-Length: 324
06:35:26.988 [main] DEBUG org.apache.http.impl.conn.DefaultManagedHttpClientConnection - http-outgoing-0: Close connection
06:35:26.988 [main] DEBUG org.apache.http.impl.execchain.MainClientExec - CONNECT refused by proxy: HTTP/1.1 400 Bad Request
06:35:26.988 [main] DEBUG org.apache.http.impl.execchain.MainClientExec - Connection discarded
06:35:26.988 [main] DEBUG org.apache.http.impl.conn.PoolingHttpClientConnectionManager - Connection released: [id: 0][route: {tls}->http://webproxy.africa.nedcor.net:9001->https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200][total available: 0; route allocated: 0 of 2; total allocated: 0 of 20]
Status: HTTP/1.1 400 Bad Request
