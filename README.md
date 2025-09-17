   ┌─────────────────────┐
   │  File Manager       │
   │     Service         │
   └─────────┬───────────┘
             │ Authenticate
             ▼
   ┌─────────────────────┐
   │ Azure AD            │
   │ (ClientID, TenantID)│
   └─────────┬───────────┘
             │ Access Token
             ▼
   ┌─────────────────────┐
   │ Azure Key Vault     │
   │   (Secrets)         │
   └─────────┬───────────┘
             │ Retrieve Secrets
             ▼
   ┌─────────────────────┐
   │ Azure Blob Storage  │
   │ (Upload/Download)   │
   └─────────────────────┘
