private String getSecretFromVault(String key, String token) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(VAULT_URL + "/v1/Store_Dev/10099");
            get.setHeader("x-vault-namespace", VAULT_NAMESPACE);
            get.setHeader("x-vault-token", token);

            try (CloseableHttpResponse response = client.execute(get)) {
                String jsonResponse = EntityUtils.toString(response.getEntity());
                JSONObject jsonObject = new JSONObject(jsonResponse);
                return jsonObject.getJSONObject("data").getString(key);
            }
        } catch (Exception e) {
            throw new RuntimeException("❌ Failed to retrieve secret from Vault", e);
        }
    }
