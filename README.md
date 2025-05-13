Caused by: java.io.FileNotFoundException: https:\nsndvextr01.blob.core.windows.net\nsnakscontregecm001\DEBTMAN.csv (The filename, directory name, or volume label syntax is incorrect)
	at java.base/java.io.FileInputStream.open0(Native Method) ~[na:na]
	at java.base/java.io.FileInputStream.open(FileInputStream.java:216) ~[na:na]
	at java.base/java.io.FileInputStream.<init>(FileInputStream.java:157) ~[na:na]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileAndGenerateSasUrl(BlobStorageService.java:71) ~[classes/:na]
	... 23 common frames omitted

2025-05-14T01:12:48.742+02:00 DEBUG 9432 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@6480b43f, headers={id=14ab38bc-7ec4-1589-82cf-dc120a868477, timestamp=1747177945024}]]
2025-05-14T01:12:48.742+02:00  INFO 9432 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T01:12:48.742+02:00  INFO 9432 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747081341.014287800,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
2025-05-14T01:12:48.743+02:00  INFO 9432 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Parsed batchId: 12345, filePath: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv, objectId: {1037A096-0000-CE1A-A484-3290CA7938C2}

Process finished with exit code -1
