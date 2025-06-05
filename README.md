Hi Mfoo,

We’ve completed the changes to switch from HashiCorp Vault to Azure Key Vault for securely retrieving the storage account name, key, and container. The updated build has been successfully deployed to the cluster.

However, we’re currently facing an issue:

🔍 Issue Observed:
We’re getting a 404 Not Found response when accessing the following URLs:

https://dev-exstream.nednet.co.za/api/file/process

https://dev-exstream.nednet.co.za/file-manager/api/file/process

https://dev-exstream.nednet.co.za/api/file/health

✅ What We’ve Verified:
Locally, the endpoints are accessible and working fine via:
http://localhost:8080/api/file/health

As you confirmed, the application is also responding properly on your Linux machine using:
curl -i localhost:9091/api/file/health

No context-path has been configured in the application properties.

Kindly help us resolve this and ensure the API is accessible through the expected URL.
