package com.nedbank.kafka.filemanage.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "proxy")
public class ProxyProperties {

    private String host;
    private String port;
    private String username;
    private String password;

    // Getters and Setters

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

package com.nedbank.kafka.filemanage.config;

import org.springframework.stereotype.Component;

import java.net.*;

@Component
public class ProxySetup {

    private final ProxyProperties proxyProperties;
    private static boolean configured = false;

    public ProxySetup(ProxyProperties proxyProperties) {
        this.proxyProperties = proxyProperties;
    }

    public void configureProxy() {
        if (configured) return;

        String host = proxyProperties.getHost();
        String port = proxyProperties.getPort();
        String username = proxyProperties.getUsername();
        String password = proxyProperties.getPassword();

        if (host == null || port == null || host.isEmpty() || port.isEmpty()) {
            System.err.println("⚠️ Proxy settings are missing. Proxy not configured.");
            return;
        }

        System.setProperty("http.proxyHost", host);
        System.setProperty("http.proxyPort", port);
        System.setProperty("https.proxyHost", host);
        System.setProperty("https.proxyPort", port);
        System.setProperty("java.net.useSystemProxies", "true");

        if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });
        }

        System.out.println("🔧 Proxy configured from ProxyProperties: " + host + ":" + port);
        configured = true;
    }
}

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ProxyProperties.class)
public class AppConfig {
}
