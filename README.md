public class VaultTester {

    public static void main(String[] args) throws Exception {
        // Proxy settings
        System.setProperty("http.proxyHost", "webproxy.africa.nedcor.net");
        System.setProperty("http.proxyPort", "9001");
        System.setProperty("https.proxyHost", "webproxy.africa.nedcor.net");
        System.setProperty("https.proxyPort", "9001");
        System.setProperty("java.net.useSystemProxies", "true");

        Authenticator.setDefault(new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("yourUsername", "yourPassword".toCharArray());
            }
        });

        // Trust all certificates if needed (for testing only, not production)
        SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(null, (chain, authType) -> true)
                .build();

        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext);

        // Configure client
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(20000)
                .setSocketTimeout(20000)
                .setRedirectsEnabled(true)
                .setMaxRedirects(10)
                .build();

        CloseableHttpClient client = HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setSSLSocketFactory(sslsf)
                .setRedirectStrategy(new LaxRedirectStrategy()) // retains original method
                .disableCookieManagement()
                .build();

        HttpGet request = new HttpGet("https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health");
        request.setHeader("Accept", "application/json");

        try (CloseableHttpResponse response = client.execute(request)) {
            System.out.println("Status Code: " + response.getStatusLine().getStatusCode());
            String responseBody = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                    .lines().collect(Collectors.joining("\n"));
            System.out.println(responseBody);
        } catch (Exception e) {
            System.err.println("❌ Error during connection:");
            e.printStackTrace();
        }
    }
}
