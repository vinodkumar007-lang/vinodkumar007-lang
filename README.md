# application.properties
proxy.host=proxyprod.africa.nedcor.net
proxy.port=80

package com.nedbank.kafka.filemanage.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.net.InetSocketAddress;
import java.net.Proxy;

@Configuration
public class RestTemplateConfig {

    @Value("${proxy.host}")
    private String proxyHost;

    @Value("${proxy.port}")
    private int proxyPort;

    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();

        if (proxyHost != null && !proxyHost.isEmpty()) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
            factory.setProxy(proxy);
        }

        return new RestTemplate(factory);
    }
}

@Service
public class VaultService {

    private final RestTemplate restTemplate;

    public VaultService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String checkVaultHealth() {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health";
        return restTemplate.getForObject(url, String.class);
    }
}
