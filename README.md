Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
        modified:   file-manager/Dockerfile
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/config/KafkaConfig.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/controller/FileProcessingController.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/ApiResponse.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/BatchFile.java
        new file:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/CustomerData.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/KafkaMessage.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/Metadata.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/Payload.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/PrintFile.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/SummaryPayload.java
        new file:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/SummaryPayloadResponse.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/SummaryProcessedFile.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/model/SummaryResponse.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/service/BlobStorageService.java
        new file:   file-manager/src/main/java/com/nedbank/kafka/filemanage/service/DataParser.java
        new file:   file-manager/src/main/java/com/nedbank/kafka/filemanage/service/FileGenerator.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/service/KafkaListenerService.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/test/KafkaTestProducer.java
        modified:   file-manager/src/main/java/com/nedbank/kafka/filemanage/utils/SummaryJsonWriter.java
        new file:   file-manager/summary.json
