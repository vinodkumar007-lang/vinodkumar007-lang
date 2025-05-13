import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.config.RequestConfig;
import java.io.IOException;

public class VaultProxyBypassTest {

    public static void main(String[] args) {

        // 🚫 Disable proxy settings explicitly
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");
        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");
        System.setProperty("java.net.useSystemProxies", "false");

        // 🕒 Optional: set longer timeout
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(20000)
                .setSocketTimeout(20000)
                .build();

        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultRequestConfig(config)
                .build()) {

            HttpGet request = new HttpGet("https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/sys/health");

            System.out.println("🔍 Attempting direct connection to Vault...");

            try (CloseableHttpResponse response = client.execute(request)) {
                System.out.println("✅ Response Code: " + response.getStatusLine().getStatusCode());
            }

        } catch (IOException e) {
            System.err.println("❌ Connection failed:");
            e.printStackTrace();
        }
    }
}
