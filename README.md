FileManager (Filemanager composition service) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
properties:					
	datastreamName				Fmcompose
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcompose
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event – links to composition application
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				bobUrl	Where data file has been copied to on azure blob storage - Get value from event received
				fileName	Get value from event received
				fileType	Get value from event received
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					


FileManager (Filemanager service complete) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
Properties:					
	datastreamName				Fmcomplete
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcomplete
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event received
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				blobUrl	Where summary file has been copied to on azure blob storage 
				fileName	Summary report name
				fileType	
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					
