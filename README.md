package com.nedbank.kafka.filemanage.config;

import java.net.*;
import java.util.*;

public class ProxySetup {
    public static void configureProxy() {
        String proxyHost = "proxyprod.africa.nedcor.net";//"webproxy.africa.nedcor.net";  // Proxy host
        String proxyPort = "80";//"9001";  // Proxy port
        String proxyUsername = "CC437236";  // Proxy username (if needed)
        String proxyPassword = "34dYaB@jEh56";  // Proxy password (if needed)

        // Set the HTTP/HTTPS proxy system properties
        System.setProperty("http.proxyHost", proxyHost);
        System.setProperty("http.proxyPort", proxyPort);
        System.setProperty("https.proxyHost", proxyHost);
        System.setProperty("https.proxyPort", proxyPort);

        // If proxy requires authentication, you can set up authentication as follows
        if (proxyUsername != null && proxyPassword != null) {
            Authenticator.setDefault(new Authenticator() {
                @Override
                public PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(proxyUsername, proxyPassword.toCharArray());
                }
            });
        }

        // Use system-wide proxy settings (optional)
        System.setProperty("java.net.useSystemProxies", "true");
    }
}
