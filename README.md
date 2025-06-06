As part of our local development, we previously used HashiCorp Vault for authentication, where we retrieved credentials such as:

accountKey

accountName = "nsndvextr01"

containerName = "nsnakscontregecm001"

These credentials were used to connect to Azure Blob Storage.

We have now migrated from HashiCorp Vault to Azure Key Vault. To support this, we require the following secrets to be available in Azure Key Vault:

AZURE_CLIENT_ID

AZURE_TENANT_ID

AZURE_CLIENT_SECRET

These will be used to authenticate and fetch the required Blob Storage keys from Azure Key Vault.

We kindly request you to create a similar setup in Azure Key Vault and add the necessary secrets in the same format. If you have any recommendations or a preferred naming convention, please let us know so we can adjust accordingly on our side.
