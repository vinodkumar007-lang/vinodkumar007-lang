String newBlobUrl = blobStorageService.copyFileFromUrlToBlob(batchFile.getBlobUrl(), targetBlobPath);

String summaryFileUrl = blobStorageService.uploadFile(summaryFile, summaryBlobPath);
