keytool -list -v -keystore "C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\keystore.jks"

keytool -list -v -keystore "C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\truststore.jks"

Export a certificate to a .crt file

keytool -exportcert -alias mycert -keystore "C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\keystore.jks" -file C:\Users\CC437236\Desktop\mycert.crt -rfc

(Optional) Convert keystore to .p12 and extract private key (if needed)

keytool -importkeystore -srckeystore "C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\keystore.jks" -destkeystore C:\Users\CC437236\Desktop\keystore.p12 -deststoretype PKCS12
