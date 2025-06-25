import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.InputStream;
import java.nio.file.*;

public class BlobToMountTest {

    public static void main(String[] args) throws Exception {
        String blobUrl = "https://<your-storage-account>.blob.core.windows.net/<container>/<path/to/file>?<sas-token>";
        String fileName = "testfile.pdf";
        String batchId = "test-batch";
        String guiRefId = "test-gui";

        Path mountPath = Paths.get("/mnt/nfs/dev-exstream/dev-SA/job", batchId, guiRefId, fileName);
        Files.createDirectories(mountPath.getParent());

        downloadBlobToPath(blobUrl, mountPath);

        System.out.println("✅ File written to: " + mountPath);
    }

    public static void downloadBlobToPath(String blobUrl, Path targetPath) throws Exception {
        BlobClient blobClient = new BlobClientBuilder()
                .endpoint(getBlobEndpoint(blobUrl))
                .sasToken(getSasToken(blobUrl))
                .containerName(getContainerName(blobUrl))
                .blobName(getBlobName(blobUrl))
                .buildClient();

        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();

        try (InputStream inputStream = blockBlobClient.openInputStream()) {
            Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("📂 Downloaded blob written to: " + targetPath);
    }

    private static String getBlobEndpoint(String blobUrl) {
        return blobUrl.substring(0, blobUrl.indexOf(".core.windows.net") + 17);
    }

    private static String getContainerName(String blobUrl) {
        String afterNet = blobUrl.split(".net/")[1];
        return afterNet.substring(0, afterNet.indexOf('/'));
    }

    private static String getBlobName(String blobUrl) {
        String afterNet = blobUrl.split(".net/")[1];
        String blobPath = afterNet.substring(afterNet.indexOf('/') + 1);
        return blobPath.contains("?") ? blobPath.substring(0, blobPath.indexOf('?')) : blobPath;
    }

    private static String getSasToken(String blobUrl) {
        return blobUrl.contains("?") ? blobUrl.substring(blobUrl.indexOf("?") + 1) : "";
    }
}
