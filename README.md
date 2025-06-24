📄 File Manager Service — Design Document
Client: Nedbank
Version: v2.3 — June 2025 (FINAL)
Prepared by: [Your team]

1️⃣ Overview
The File Manager Service automates the processing of driver files from Kafka, integrates with OpenText system, retrieves final processed file URLs, builds summary.json, connects to Azure Key Vault, uploads summary to Azure Blob Storage, and sends final Kafka message.

Deployed on Azure AKS. Integrates with Kafka, OpenText, Key Vault, Blob Storage.

2️⃣ Architecture
Component	Description
Kafka (Input Topic)	Driver messages (DATA/REF)
File Manager Service	KafkaListener → OpenText API → Build summary.json → Key Vault → Blob upload
OpenText System	Processes driver files, uploads output files to Blob, returns URLs
Azure Key Vault	Stores Blob secrets
Azure Blob Storage	Stores summary.json
Kafka (Output Topic)	Sends final summary message

3️⃣ End-to-End Flow (v2.3 FINAL)
1️⃣ Kafka message received — @KafkaListener consumes new message

2️⃣ Parse message fields — batchID, fileName, fileType (DATA/REF), deliveryType, blobURL

3️⃣ DATA/REF logic:

If 1 DATA + REF(s) → process only DATA

If only REF → skip batch

4️⃣ Parse DATA file — extract customer records (05 records), deliveryType (PRINT/EMAIL/MOBSTAT)

5️⃣ Call OpenText API — send driver message to OpenText system

6️⃣ OpenText processes files — using Blob URL from Kafka message

7️⃣ OpenText uploads output files → to Blob
- /archive
- /email
- /html
- /mobstat
- /print

8️⃣ OpenText sends API response → to File Manager — with Blob URLs of generated files

9️⃣ On receiving OT response:

markdown
Copy
Edit
- **Prepare summary.json**:
    - batchID  
    - fileName  
    - header info  
    - processedFiles[]  
    - printFiles[] (from OT returned URLs)  
🔟 Connect to Azure Key Vault:
- Authenticate using tenant ID + client ID
- Retrieve secrets:
- storageAccountName
- storageAccountKey
- containerName

1️⃣1️⃣ Connect to Azure Blob Storage using secrets

1️⃣2️⃣ Upload summary.json → to /summary/ folder

1️⃣3️⃣ Send Kafka result message to output topic

1️⃣4️⃣ Commit Kafka offset after success

4️⃣ Kafka Consumer Design
Uses Spring KafkaListener (@KafkaListener)

Auto-polling in background

Config:

enable.auto.commit = false

auto.offset.reset = earliest

Flow:

Consume → Call OT → Build summary → Upload summary → Send Kafka msg → Commit offset

5️⃣ Processing Logic
5.1 Message Fields
Field	Example
batchID	BATCH123
fileName	data_file.dat
fileType	DATA / REF
deliveryType	PRINT/EMAIL/MOBSTAT
blobURL	https://...
tenantCode	T123
channelID	...

5.2 DATA / REF Logic
Case	Action
DATA + REF(s)	Process only DATA
Only REF	Skip batch

5.3 Delivery Types
deliveryType	Output Location
PRINT	/print
EMAIL	/email
MOBSTAT	/mobstat

6️⃣ Folder Structure — Azure Blob (by OpenText)
Folder	Contents
/archive	Original driver file
/email	Email files
/html	HTML files
/mobstat	Mobstat files
/print	Print files
/summary	summary.json (by File Manager)

7️⃣ summary.json Structure
json
Copy
Edit
{
  "batchID": "BATCH123",
  "fileName": "data_file.dat",
  "header": {
    "tenantCode": "...",
    "channelID": "..."
  },
  "processedFiles": [
    { "fileURL": "...", "status": "SUCCESS" }
  ],
  "printFiles": [
    { "printFileURL": "...", "status": "SUCCESS" }
  ],
  "summaryFileURL": "...",
  "timestamp": "2025-06-24T12:34:56"
}
8️⃣ Components
Component	Role
KafkaListenerService	Consume Kafka msg, trigger flow
OpenText API Client	Send msg to OT, receive file URLs
AzureKeyVaultClient	Retrieve secrets
BlobStorageService	Upload summary.json
SummaryJsonWriter	Build summary.json
KafkaProducer	Send result Kafka msg

9️⃣ Deployment
Platform: Azure AKS

Kafka: SSL secured

Key Vault: used for secret retrieval

Blob Storage: stores only summary.json

Logs → Azure Monitor

10️⃣ Version History
Version	Date	Changes
v1.0	May 2025	Initial design
v2.0	June 2025	Added DATA/REF logic, KafkaListener
v2.1	June 2025	Added OpenText API step
v2.2	June 2025	Added Key Vault step
v2.3	June 2025	Final flow: OT uploads files, FileManager builds summary.json
