Thank you for your support in updating the Client ID and Tenant ID.

Currently, we are facing an Access Denied error when attempting to read secrets (e.g., ecm-fm-account-key) from the Key Vault.

Kindly request you to grant 'Secrets - Get' permission to the following AppId:

makefile
Copy
Edit
AppId: 1ba2b144-328e-4185-9305-3b43bc317449
KeyVault: NSN-DEV-ECM-KVA-001
via Access Policy or RBAC, so the AKS application can read secrets as required.

Also, please ensure that we have the correct Account Name, Account Key, and Container Name for Blob Storage connectivity.

Appreciate your assistance.
