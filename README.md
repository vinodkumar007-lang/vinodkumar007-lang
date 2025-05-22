✅ 1. Ensure the setting is active
Run:

powershell
Copy
Edit
az config get core.disable_ssl_validation
It should output:

json
Copy
Edit
[
  {
    "name": "core.disable_ssl_validation",
    "value": true
  }
]
If it doesn’t show true, set it again:

powershell
Copy
Edit
az config set core.disable_ssl_validation=true
✅ 2. Try setting environment variables for proxy and disabling SSL
Set everything in the same session:

powershell
Copy
Edit
$env:HTTP_PROXY = "http://CC437236:34dYaB@jEh56@proxyprod.africa.nedcor.net:80"
$env:HTTPS_PROXY = "http://CC437236:34dYaB@jEh56@proxyprod.africa.nedcor.net:80"
$env:REQUESTS_CA_BUNDLE = ""
az config set core.disable_ssl_validation=true
az login
REQUESTS_CA_BUNDLE being blank ensures no cert is required (this overrides any cert path).

✅ 3. Check if a system-level proxy is interfering
Try this:

powershell
Copy
Edit
[System.Net.WebRequest]::DefaultWebProxy.Credentials
If it shows default credentials, your system proxy might still be affecting the CLI.

✅ 4. Try login with device code (alternative login method)
Sometimes, az login via browser works better behind a proxy:

powershell
Copy
Edit
az login --use-device-code
This gives you a code and opens a webpage to enter it — which sometimes works even when direct login fails.
