keytool -import -alias vault-cert -file /path/to/vault-cert.crt -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit
keytool -list -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit
