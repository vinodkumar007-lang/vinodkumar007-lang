package com.nedbank.kafka.filemanage.service;

import org.springframework.web.client.RestTemplate;

public class VaultService {

    private final RestTemplate restTemplate;

    public VaultService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String checkVaultHealth() {
        String url = "https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health";
        return restTemplate.getForObject(url, String.class);
    }

    public static void main(String[] args) {
        // Manually configure proxy if needed
        System.setProperty("http.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("http.proxyPort", "80");
        System.setProperty("https.proxyHost", "proxyprod.africa.nedcor.net");
        System.setProperty("https.proxyPort", "80");

        // Create RestTemplate and test
        RestTemplate restTemplate = new RestTemplate();
        VaultService vaultService = new VaultService(restTemplate);

        try {
            String response = vaultService.checkVaultHealth();
            System.out.println("Vault health response:\n" + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
