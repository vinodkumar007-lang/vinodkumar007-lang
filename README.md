C:\Users\CC437236>keytool -import -alias hasicorp-cert -file "C:/Users/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/hashicorp.crt" -keystore "C:/Users/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/cacerts" -storepass changeit
Certificate already exists in keystore under alias <vault-cert>
Do you still want to add it? [no]:  yes
Certificate was added to keystore

Warning:
The JKS keystore uses a proprietary format. It is recommended to migrate to PKCS12 which is an industry standard format using "keytool -importkeystore -srckeystore C:/Users/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/cacerts -destkeystore C:/Users/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/cacerts -deststoretype pkcs12".
