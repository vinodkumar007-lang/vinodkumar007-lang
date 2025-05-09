private String getSecretFromVault(String key, String token) {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
        HttpPost post = new HttpPost(VAULT_URL + "/v1/Store_Dev/10099");
        post.setHeader("x-vault-namespace", VAULT_NAMESPACE);
        post.setHeader("x-vault-token", token);
        post.setHeader("Content-Type", "application/json");

        // Add body with password
        String jsonBody = "{ \"password\": \"nbh_dev1\" }";
        post.setEntity(new StringEntity(jsonBody));

        try (CloseableHttpResponse response = client.execute(post)) {
            String jsonResponse = EntityUtils.toString(response.getEntity());
            JSONObject jsonObject = new JSONObject(jsonResponse);
            return jsonObject.getJSONObject("data").getString(key);
        }
    } catch (Exception e) {
        throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
    }
}
