I’m currently working on integrating our Kafka connection with an orchestration tool, and I need clarification on the client authentication setup.

Our Java application is currently configured to connect using SSL/mTLS, with the following keystore:

pgsql
Copy
Edit
keystore location: C:\Users\CC437236\jdk-17.0.12_windows-x64_bin\jdk-17.0.12\lib\security\keystore.jks
Could you please confirm:

Is mutual TLS (mTLS) being used for authentication?

If so, what identity/username (e.g., from the client certificate CN or SAN) is registered for this client on the Kafka side?

If there’s a specific username/password required for use in orchestration (e.g., if we need to switch to SASL for that integration), can you please provide or provision those credentials?

Let me know if there’s a secure way to retrieve or store these credentials (e.g., through Vault or Key Vault), and if we need to align on any certificate rotation policies.
