The overall end-to-end file manager flow has been implemented and tested successfully with a limited set of messages
Bulk file testing (batch-wise) is pending, as currently we are observing only limited Kafka messages in the consumer topic
We are still awaiting access to the Azure Blob Storage account to verify and confirm whether files are being stored correctly
Azure repository access is also pending, which is required to push the code related to this module
Docker image creation is on hold, pending confirmation regarding Docker setup and access
Some fields required to generate the final summary file are missing in the Kafka consumer messages These are needed before we can publish to the output topic
Additionally, the valid file path in the summary file needs to be updated after the OT process is completed

