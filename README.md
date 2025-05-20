✅ Step-by-Step Git CMD Proxy Setup
1. Set Proxy for Git (not environment-wide)
In Git CMD, instead of setting environment variables with set, it’s better to configure Git directly:

cmd
Copy
Edit
git config --global http.proxy http://CC437236:34dYaB%40jEh56@proxyprod.africa.nedcor.net:80
git config --global https.proxy http://CC437236:34dYaB%40jEh56@proxyprod.africa.nedcor.net:80
📝 %40 is the encoded @ in your password. If your password has other special characters (like !, #, or $), they might also need URL encoding.

You can verify it was set:

cmd
Copy
Edit
git config --global --get http.proxy
git config --global --get https.proxy
2. Clone the Git Repository
After setting the proxy:

cmd
Copy
Edit
git clone https://dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Exstream_Dev
3. Optional: Clear Proxy Later
If you ever want to remove the proxy settings:

cmd
Copy
Edit
git config --global --unset http.proxy
git config --global --unset https.proxy
🛠 Troubleshooting Tips
If authentication fails, double-check your username and password.

Consider using a Personal Access Token (PAT) instead of a password for Azure DevOps—it’s more secure and often required.

You can structure the clone like this using a PAT:

cmd
Copy
Edit
git clone https://<username>:<PAT>@dev.azure.com/Nedbank-Limited/ECM_Exstream/_git/ECM_comp_Exstream_Dev
Replace <PAT> with your personal access token.

Would you like help generating a PAT or URL-encoding your actual password safely?
