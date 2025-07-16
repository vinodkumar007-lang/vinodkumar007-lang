✅ Step 2: Send Kafka Message to OpenText 
• File-Manager constructs a Kafka message containing: 

Metadata about the file.(contains updated kafka message ) 

Instructions for OpenText to process the file. 
• Sends the message to API service call: 
https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/batch/dev-SA/ECPDebtmanService 
