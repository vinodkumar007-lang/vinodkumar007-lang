File-Manager Service: Quick Access & Configuration Checklist
1️⃣ Kafka

Check input topic access and validate permissions.

Check output topic access and validate permissions.

Check consumer group configuration and access.

Configure and validate SSL certificates: keystore.jks, truststore.jks.

Install required certificates on environment.

Check authentication credentials for Kafka.

Validate read/write permissions for topics.

2️⃣ Azure Blob Storage

Check AZURE_CLIENT_ID is configured.

Check AZURE_TENANT_ID is configured.

Check account key / client secret is available.

Check account name is correct.

Check mount path access for temporary file storage.

Containers & Access:

Check access to archive container (original input files).

Check access to email container (EMAIL files).

Check access to print container (PRINT files).

Check access to mobstat container (mobile/status files).

Check access to summary container (summary.json files).

Validate read access for input files.

Validate write access for processed files.

Validate write access for summary.json files.

Validate proper naming conventions inside each container.

3️⃣ Azure AD / App Registration

Check File-Manager service is registered in Azure AD directory.

Check Azure Client ID from app registration.

Check Azure Tenant ID for the directory.

Validate app permissions for Blob Storage, KeyVault, OpenText.

Validate app registration credentials can access required resources.

4️⃣ KeyVault

Check access to fetch OTDS token.

Check access to fetch Azure credentials if stored in KeyVault.

Validate read permissions on KeyVault.

5️⃣ OpenText Service

Check OTDS token availability and validity.

Check access to call OpenText APIs.

Validate network/firewall access to OT endpoint.

6️⃣ File / Folder Permissions

Check mount path read/write access for downloaded files.

Check temporary path storage availability.

Validate permissions for generating and storing processed files.

7️⃣ Summary Generation

Check access to Blob Storage to write summary.json.

Validate uploaded summary URL accessibility.

Check naming conventions for summary files per batch.

8️⃣ General / Validation

Check logging is enabled for each step.

Validate no duplicate processing of Kafka messages.

Check all environment variables are correctly set.

Validate SSL for all connections (Kafka, Blob Storage, OpenText).
