PS C:\Users\CC437236> az config set core.disable_ssl_validation=true
WARNING: Command group 'config' is experimental and under development. Reference and support levels: https://aka.ms/CLI_refstatus
PS C:\Users\CC437236> az config get core.disable_ssl_validation
WARNING: Command group 'config' is experimental and under development. Reference and support levels: https://aka.ms/CLI_refstatus
{
  "name": "disable_ssl_validation",
  "source": "C:\\Users\\CC437236\\.azure\\config",
  "value": "true"
}
PS C:\Users\CC437236> $env:HTTP_PROXY = "http://CC437236:34dYaB@jEh56@proxyprod.africa.nedcor.net:80"
PS C:\Users\CC437236> $env:HTTPS_PROXY = "http://CC437236:34dYaB@jEh56@proxyprod.africa.nedcor.net:80"
PS C:\Users\CC437236> az login
ERROR: HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate in certificate chain (_ssl.c:997)')))
Certificate verification failed. This typically happens when using Azure CLI behind a proxy that intercepts traffic with a self-signed certificate. Please add this certificate to the trusted CA bundle. More info: https://docs.microsoft.com/cli/azure/use-cli-effectively#work-behind-a-proxy.
PS C:\Users\CC437236> az login --use-device-code
ERROR: HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate in certificate chain (_ssl.c:997)')))
Certificate verification failed. This typically happens when using Azure CLI behind a proxy that intercepts traffic with a self-signed certificate. Please add this certificate to the trusted CA bundle. More info: https://docs.microsoft.com/cli/azure/use-cli-effectively#work-behind-a-proxy.
