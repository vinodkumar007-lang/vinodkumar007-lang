FileManager Application – Updates Summary
1. HTML / Text File Handling

All HTML or text files available in the Email folder are now picked up by the system.

These files are stored in Blob Storage along with the corresponding email PDF files.

The stored files are included in the summary.json under the relevant email entries.

2. Print_Report File Handling

Print_Report files are excluded from:

the Print folder in Blob Storage

the summary.json

This ensures only relevant files are tracked in summary while Print_Report files are handled separately if needed.

3. Audit Tracking

Audit topic implemented to track FileManager operations:

FmConsume: logs all input message details when processing starts.

FmComplete: logs completion of processing and publishes the output message to the audit topic.

This provides full traceability of file processing from input reception to completion.
