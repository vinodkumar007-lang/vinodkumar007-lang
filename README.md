✅ Configuring Git to Use Your Proxy
Here's how to set Git to work with your proxy:

1. Set Proxy in Git Globally
Open a Command Prompt and run:

bash
Copy
Edit
git config --global http.proxy http://CC437236:34dYaB@jEh56@proxyprod.africa.nedcor.net:80
⚠️ Note: The @ in your password must be URL-encoded as %40.

So the correct command becomes:

bash
Copy
Edit
git config --global http.proxy http://CC437236:34dYaB%40jEh56@proxyprod.africa.nedcor.net:80
🔒 Security Tip
To avoid putting your password in plain text, you can also use the Windows Credential Manager or omit the password from Git config and input it interactively when prompted.

✅ Alternative: Set Environment Variables (Temporary)
If you prefer not to use Git config:

cmd
Copy
Edit
set HTTP_PROXY=http://CC437236:34dYaB%40jEh56@proxyprod.africa.nedcor.net:80
set HTTPS_PROXY=http://CC437236:34dYaB%40jEh56@proxyprod.africa.nedcor.net:80
Then try the Git clone again:

cmd
Copy
Edit
git clone https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Exstream_Dev
🧹 To Remove Proxy Later
If you no longer need the proxy settings:

bash
Copy
Edit
git config --global --unset http.proxy
