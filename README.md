2025-05-27T16:17:43.897+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Received message: {
  "BatchId" : "f230ccff-7ba6-4969-b00c-4dde811f69f2",
  "SourceSystem" : "DEBTMAN",
  "TenantCode" : "ZANBL",
  "ChannelID" : null,
  "AudienceID" : null,
  "Product" : "DEBTMAN",
  "JobName" : "DEBTMAN",
  "UniqueConsumerRef" : "f622bb35-ad57-4810-983b-f4d5659fb595",
  "Timestamp" : 1748351452.155529400,
  "RunPriority" : null,
  "EventType" : null,
  "BatchFiles" : [ {
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH",
    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "Filename" : "DEBTMAN.csv",
    "ValidationStatus" : "valid"
  } ]
}
2025-05-27T16:17:43.898+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Processing message from topic str-ecp-batch-composition, partition=0, offset=18542
2025-05-27T16:17:43.898+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Processing batchId=f230ccff-7ba6-4969-b00c-4dde811f69f2, sourceSystem=DEBTMAN
2025-05-27T16:17:43.898+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Copying file from 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv' to 'DEBTMAN/input/2025-05-27T13-10-52Z/f230ccff-7ba6-4969-b00c-4dde811f69f2/f622bb35-ad57-4810-983b-f4d5659fb595_DEBTMAN/DEBTMAN.csv'
2025-05-27T16:17:44.033+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : ✅ Copied 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv' to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2Finput%2F2025-05-27T13-10-52Z%2Ff230ccff-7ba6-4969-b00c-4dde811f69f2%2Ff622bb35-ad57-4810-983b-f4d5659fb595_DEBTMAN%2FDEBTMAN.csv'
2025-05-27T16:17:44.034+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.KafkaListenerService     : Summary JSON written locally at C:\Users\CC437236\AppData\Local\Temp\summary.json
2025-05-27T16:17:44.100+02:00  INFO 11128 --- [nio-8080-exec-2] c.n.k.f.service.BlobStorageService       : ✅ Uploaded file to 'https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN%2Fsummary%2Ff230ccff-7ba6-4969-b00c-4dde811f69f2%2Fsummary.json'
