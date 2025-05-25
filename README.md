	
Field
	
Description
	
Type
	
Kafka Topic
	
Remarks

 	
BatchID (golden thread)
	
Unique identifier for the batch.
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
TenantCode (golden thread)
	
Tenant identifier (e.g., ZANBL).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publisg message

 	
ChannelID (golden thread)
	
Channel ID (e.g., 100 = consumer).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet
	
AudienceID (golden thread)
	
GUID for audience, often for security.
	
GUID
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Timestamp
	
Time the event occurred.
	
DateTime
	
Both
	
Kafka generated

 	
SourceSystem (golden thread)
	
Source system of origin (e.g., CARD).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Product (golden thread)
	
Product associated with the batch (e.g., CASA).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
JobName (golden thread)
	
Job identifier (e.g., SMM815).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
UniqueConsumerRef (golden thread)
	
GUID for identifying the consumer.
	
GUID
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message

 	
Blob URL (storage name where input data is)
	
GUID for identifying the ECP batch.
	
GUID
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition

 	
Filename on blob storage 
	
 
	
 
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition


not in use yet
	
RunPriority
	
Batch priority (High, Medium, Low).
	
String
	
str-ecp-batch-composition
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet
	
EventType
	
Type of event (e.g., Completion, Restart).
	
String
	
Both
	
ECP process will populate on str-ecp-batch-composition, use in publish message


not in use yet, rerun/reprocess to be defined
	
RestartKey
	
Key used to support restart events.
	
String
	
str-ecp-batch-composition
	
First field customer

 	
TotalFilesProcessed
	
Number of successfully processed files.
	
Integer
	
str-ecp-batch-composition-complete
	
OT team

 	
ProcessingStatus
	
Overall status (Success, Failure, Partial).
	
String
	
str-ecp-batch-composition-complete
	
OT team


what are the valid values? Eg. RC = 00. To be defined during testing
	
EventOutcomeCode
	
Code indicating result (Success, Tech Error, etc.).
	
String
	
str-ecp-batch-composition-complete
	
OT team

 	
EventOutcomeDescription
	
Human-readable explanation of the outcome.
	
String
	
str-ecp-batch-composition-complete
	
OT team

 	
SummaryReportFileLocation
	
URL or path of the summary report file.
	
URL
	
str-ecp-batch-composition-complete
	
OT team
