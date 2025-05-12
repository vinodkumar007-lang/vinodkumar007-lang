package com.nedbank.vault.diagnostic;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

@SpringBootApplication
public class VaultConnectivityDiagnosticApp implements CommandLineRunner {

    // Vault URL
    private static final String VAULT_URL = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health";

    // Proxy Configuration
    private static final String PROXY_HOST = "webproxy.africa.nedcor.net";
    private static final int PROXY_PORT = 9001;

    // Optional proxy credentials
    private static final String PROXY_USERNAME = "yourProxyUsername"; // replace or leave null
    private static final String PROXY_PASSWORD = "yourProxyPassword"; // replace or leave null

    public static void main(String[] args) {
        SpringApplication.run(VaultConnectivityDiagnosticApp.class, args);
    }

    @Override
    public void run(String... args) {
        try {
            // Set up proxy authentication if needed
            if (PROXY_USERNAME != null && PROXY_PASSWORD != null) {
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(PROXY_USERNAME, PROXY_PASSWORD.toCharArray());
                    }
                });
            }

            HttpHost proxy = new HttpHost(PROXY_HOST, PROXY_PORT);

            RequestConfig config = RequestConfig.custom()
                    .setProxy(proxy)
                    .setConnectTimeout(15000)
                    .setSocketTimeout(15000)
                    .build();

            try (CloseableHttpClient httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(config)
                    .build()) {

                HttpGet request = new HttpGet(VAULT_URL);

                System.out.println("🔍 Attempting to connect to Vault via proxy...");
                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    String body = EntityUtils.toString(response.getEntity());
                    System.out.println("✅ Response code: " + response.getStatusLine().getStatusCode());
                    System.out.println("✅ Response body: " + body);
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Connection failed:");
            e.printStackTrace();
        }
    }
}
