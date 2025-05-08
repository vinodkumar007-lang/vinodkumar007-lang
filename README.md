I am currently exploring Azure Blob Storage functionality and have successfully uploaded a file using code.

However, when attempting to access the file via the following direct URL:

🔗 https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/dummy-file.txt

I encountered the following error:

vbnet
Copy
Edit
Status: PublicAccessNotPermitted  
Message: Public access is not permitted on this storage account.  
RequestId: 8907c508-c01e-000c-1129-c03f9f000000  
Timestamp: 2025-05-08T14:56:41.4964354Z
This error indicates that public (anonymous) access is disabled for the storage account, preventing access to the blob through unauthenticated requests.

Next Steps:
I am trying to explore the option to provide a secure, time-limited SAS (Shared Access Signature) URL that will allow read-only access to the file. This approach maintains security while enabling access for validation or review purposes.

Alternatively, if appropriate and in line with your organization’s security policies, public access can be enabled either at the container level or storage account level to allow direct access to blobs.
