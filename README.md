⏳ Step 2: Wait & Poll for Output from OT
Once OT starts processing:

🔍 FileManager behavior:
It does not block the Kafka thread. Instead:

A background thread (Executor Service) is triggered.

That thread:

Polls OT’s job folder periodically:

bash
Copy
Edit
/mnt/kafka/jobs/{jobId}/{id}/docgen/
It waits for the _STDDELIVERYFILE.xml file which signals that OT finished processing.

⏱ Polling Strategy:
Waits up to rpt.max.wait.seconds (configurable).

Polls every rpt.poll.interval.millis milliseconds.

Ensures the XML file is fully written (file size stabilized before using it).

📄 Step 3: Post-OT Processing
After OT finishes processing and generates output files:

FileManager:

Parses the XML to extract customer + output method mapping.

Matches and uploads all generated output files (EMAIL, PRINT, MOBSTAT, etc.) from the output mount path to Azure Blob.

Generates a detailed summary.json including:

Processed file status and blob URLs.

Print files.

Mobstat trigger file if present.

Uploads the summary.json to Azure Blob.
