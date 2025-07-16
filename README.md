✅ Step 2: Download File from Kafka Message, Place in Mount Path, Update Message, and Send to OpenText
📁 Download File and Place in Mount Path
For each file in the incoming Kafka message:

The file is downloaded using blobStorageService.downloadFileContent(blobUrl).

The content is stored locally at:

swift
Copy
Edit
{mountPath}/input/{sourceSystem}/{batchId}/{filename}
Example:

swift
Copy
Edit
/mnt/kafka/input/DEBTMAN/12345/InputFile1.txt
✏️ Update Kafka Message
After downloading:

The blobUrl field in each batchFile is updated to point to the local mount path where the file was stored.

java
Copy
Edit
file.setBlobUrl(localPath.toString());
🚀 Send Updated Kafka Message to OpenText
The updated message now contains:

Metadata (batchId, sourceSystem, filenames)

Mount path location of downloaded file(s)

Instructions for OpenText to process it.

The service sends this message as a POST request to:

bash
Copy
Edit
https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/batch/dev-SA/ECPDebtmanService
with required authorization headers.

