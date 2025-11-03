
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
					
					


Distribution service (archive) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
Properties:					
	datastreamName				DistributionArchive
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				DistributionArchive
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
				blobUrl	Where tar file has been copied to on azure blob storage for file transfer
				fileName	Tar file name
				fileType	
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					

