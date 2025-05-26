import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.*;
import java.net.SocketException;

public class AzureBlobUploadDemo {

    // Replace with your actual account info
    private static final String ACCOUNT_NAME = "nsndvextr01";
    private static final String ACCOUNT_KEY = "<YOUR_ACCOUNT_KEY>";
    private static final String CONTAINER_NAME = "nsnakscontregecm001";

    public static void main(String[] args) {
        String batchId = "batch123";
        String objectId = "object456";
        // The source file name already uploaded on blob storage container, here simulating
        String sourceBlobFileName = "testfile.txt"; 

        try {
            String uploadedBlobUrl = uploadFileAndReturnLocation(sourceBlobFileName, batchId, objectId);
            System.out.println("Uploaded file URL: " + uploadedBlobUrl);
        } catch (Exception e) {
            System.err.println("Upload failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static String uploadFileAndReturnLocation(String fileLocation, String batchId, String objectId) throws Exception {
        if (fileLocation == null || batchId == null || objectId == null) {
            throw new IllegalArgumentException("Required parameters missing");
        }

        String extension = getFileExtension(fileLocation);
        String blobName = batchId + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + extension;

        // Create BlobServiceClient
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(ACCOUNT_NAME, ACCOUNT_KEY);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s.blob.core.windows.net", ACCOUNT_NAME))
                .credential(credential)
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);

        // Source blob client
        BlobClient sourceBlobClient = containerClient.getBlobClient(fileLocation);
        if (!sourceBlobClient.exists()) {
            throw new FileNotFoundException("Source blob " + fileLocation + " does not exist");
        }

        // Target blob client with folder structure in name
        BlobClient targetBlobClient = containerClient.getBlobClient(blobName);

        // Open stream from source blob and upload to target blob
        try (InputStream inputStream = sourceBlobClient.openInputStream()) {
            long size = sourceBlobClient.getProperties().getBlobSize();
            targetBlobClient.upload(inputStream, size, true);
            System.out.println("Uploaded '" + fileLocation + "' to '" + targetBlobClient.getBlobUrl() + "'");
        } catch (BlobStorageException | SocketException e) {
            throw new Exception("Blob storage/network error: " + e.getMessage(), e);
        }

        // Return uploaded blob URL (no SAS token)
        return targetBlobClient.getBlobUrl();
    }

    private static String getFileExtension(String fileName) {
        int lastDotIndex = fileName.lastIndexOf('.');
        return (lastDotIndex > 0) ? fileName.substring(lastDotIndex) : "";
    }
}
