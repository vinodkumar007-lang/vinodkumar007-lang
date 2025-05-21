PS C:\Users\CC437236> az config set core.no_color=true
Command group 'config' is experimental and under development. Reference and support levels: https://aka.ms/CLI_refstatus
PS C:\Users\CC437236> az config set global.disable_ssl_validation=true
WARNING: Command group 'config' is experimental and under development. Reference and support levels: https://aka.ms/CLI_refstatus
PS C:\Users\CC437236> az login
ERROR: HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate in certificate chain (_ssl.c:997)')))
Certificate verification failed. This typically happens when using Azure CLI behind a proxy that intercepts traffic with a self-signed certificate. Please add this certificate to the trusted CA bundle. More info: https://docs.microsoft.com/cli/azure/use-cli-effectively#work-behind-a-proxy.
PS C:\Users\CC437236>
