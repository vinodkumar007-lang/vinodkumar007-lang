import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.*;

import java.net.InetAddress;

public class ProxyAccessExample {
    public static void main(String[] args) throws Exception {
        String proxyHost = "webproxy.africa.nedcor.net";
        int proxyPort = 9001;
        String username = "CC437236";
        String password = "34dYaB@jEh56";
        String domain = "NEDCORE";
        String workstation = InetAddress.getLocalHost().getHostName();

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
            new AuthScope(proxyHost, proxyPort),
            new NTCredentials(username, password, workstation, domain)
        );

        CloseableHttpClient httpClient = HttpClients.custom()
            .setDefaultCredentialsProvider(credsProvider)
            .setProxy(new HttpHost(proxyHost, proxyPort))
            .build();

        HttpGet request = new HttpGet("https://www.hashicorp.com");
        request.setHeader("User-Agent", "Mozilla/5.0"); // Optional: helps with corporate proxies

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            System.out.println("Status: " + response.getStatusLine());
        }
    }
}
