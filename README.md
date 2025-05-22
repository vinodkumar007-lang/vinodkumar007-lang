/input/{sourceSystem}/{YYYY}/{MM}/{DD}/{batchId}/{consumerReference}_{processReference}/{fileName}

input/: Indicates staging area for unprocessed files.

{sourceSystem}: Like DEBTMAN.

{YYYY}/{MM}/{DD}: From the timestamp.

{batchId}: From ObjectId, a UUID uniquely identifying the batch.

{consumerReference}_{processReference}: For message traceability.

{fileName}: Actual file name like DEBTMAN.csv.

{
  "sourceSystem": "DEBTMAN",
  "timestamp": 1747056246.229279100,
  "batchFiles": [{
    "fileLocation": "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus": "valid",
    "ObjectId": "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId": "BATCH"
  }],
  "consumerReference": "12345",
  "processReference": "Test12345",
  "batchControlFileData": null
}

/input/DEBTMAN/2025/04/29/{1037A096-0000-CE1A-A484-3290CA7938C2}/12345_Test12345/DEBTMAN.csv

Proposed Output Folder Structure (Post-Processing):

/output/{sourceSystem}/{YYYY}/{MM}/{DD}/{batchId}/{consumerReference}_{processReference}/{status}/{fileName}

output/: Top-level folder for all processed files.

{sourceSystem}: For example, DEBTMAN.

{YYYY}/{MM}/{DD}: Processing date (can reuse input timestamp or actual processing time).

{batchId}: The batch’s unique identifier (ObjectId).

{consumerReference}_{processReference}: Uniquely identifies message origin.

{status}: Processing result. E.g.,

success – processed correctly

error – failed during OT processing

partial – some records failed, some succeeded (if applicable)

fileName: Usually the same name as the original file (DEBTMAN.csv).

 Example Path After Successful Processing:

/output/DEBTMAN/2025/04/29/{1037A096-0000-CE1A-A484-3290CA7938C2}/12345_Test12345/success/DEBTMAN.csv
And if the OT processing failed:

/output/DEBTMAN/2025/04/29/{1037A096-0000-CE1A-A484-3290CA7938C2}/12345_Test12345/error/DEBTMAN.csv
