Print Files Array (printFiles[]) — Updated Structure & Behavior
Purpose
The printFiles[] section in summary.json captures details of PRINT delivery channel output files generated during Kafka message processing.
This enables tracking of file name, storage location, and generation/upload status for each print job.

Structure
Each object in the printFiles[] array contains:

Field Name	Type	Description	Example Value
printFileName	String	Name of the generated print file (usually customer-specific).	"Customer123.pdf"
printFileURL	String	Azure Blob Storage URL where the print file is stored (new destination URL after upload).	"https://<storage-account>.blob.core.windows.net/print/Customer123.pdf"
printStatus	String	Status of the print file creation & upload process. Values: "SUCCESS", "FAILED", "PARTIAL".	"SUCCESS"

Example
json
Copy
Edit
"printFiles": [
  {
    "printFileName": "Customer123.pdf",
    "printFileURL": "https://blob.company.com/container/print/Customer123.pdf",
    "printStatus": "SUCCESS"
  },
  {
    "printFileName": "Customer456.pdf",
    "printFileURL": "https://blob.company.com/container/print/Customer456.pdf",
    "printStatus": "FAILED"
  }
]
Behavior
Population Rules
Generated only for records in the input file where the delivery channel (field[4] in the 05 record) is "PRINT".

Populated during the Kafka message processing step in KafkaListenerService.

printStatus is determined after:

Generating the print file (PDF / HTML / TXT as required).

Uploading the file to Azure Blob Storage in the /print/ folder.

Status Values
Status	Meaning
SUCCESS	File generated and uploaded without errors.
FAILED	File generation or upload failed (error logged).
PARTIAL	File processed partially (e.g., generated but upload failed midway).

Blob Storage Path
Print files are stored under the /print/ subfolder of the configured container.

Example:

php-template
Copy
Edit
https://<storage-account>.blob.core.windows.net/<container-name>/print/<filename>
Duplicate Prevention
Logic ensures no duplicate entries are written for the same:

printFileName

printFileURL
