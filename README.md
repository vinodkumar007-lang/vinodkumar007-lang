package com.nedbank.kafka.filemanage.service;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

public class VaultService {

    private final RestTemplate restTemplate;

    public VaultService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String loginToVault() {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/auth/userpass/login/espire_dev";

        // Set HTTP headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("x-vault-namespace", "admin/espire");

        // JSON payload
        Map<String, String> body = new HashMap<>();
        body.put("password", "Dev+Cred4#");  // Use your real password

        HttpEntity<Map<String, String>> requestEntity = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    requestEntity,
                    String.class
            );
            return response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
            return "Login failed: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        // Optional: Configure proxy
        System.setProperty("http.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("http.proxyPort", "80");
        System.setProperty("https.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("https.proxyPort", "80");

        // Create and use service
        RestTemplate restTemplate = new RestTemplate();
        VaultService vaultService = new VaultService(restTemplate);

        String loginResponse = vaultService.loginToVault();
        System.out.println("Vault Login Response:\n" + loginResponse);
    }
}
