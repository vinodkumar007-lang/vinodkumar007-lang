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

    public String checkVaultHealth() {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health";
        return restTemplate.getForObject(url, String.class);
    }

    public String loginWithUserpass(String username, String password) {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/auth/userpass/login/" + username;

        // Request headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("x-vault-namespace", "admin/espire");  // Set your namespace

        // Request body
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("password", password);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
            return response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
            return "Login failed: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        // Proxy settings
        System.setProperty("http.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("http.proxyPort", "80");
        System.setProperty("https.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("https.proxyPort", "80");

        RestTemplate restTemplate = new RestTemplate();
        VaultService vaultService = new VaultService(restTemplate);

        // Check Vault health
        System.out.println("Vault Health:\n" + vaultService.checkVaultHealth());

        // Login using userpass
        String username = "espire_dev";           // <-- Replace with actual username
        String password = "your_actual_password"; // <-- Replace with actual password
        System.out.println("\nVault Login Response:\n" + vaultService.loginWithUserpass(username, password));
    }
}
