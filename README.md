byte[] content = Files.readAllBytes(filePath);
String blobUrl = blobStorageService.uploadFile(content, targetPath);
