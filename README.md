import java.nio.file.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MountWriteTest {

    public static void main(String[] args) {
        String batchId = "test-batch-001";
        String guiRefId = "test-gui-001";
        String fileName = "dummyfile.txt";

        // Define full target path
        Path targetPath = Paths.get("/mnt/nfs/dev-exstream/dev-SA/job", batchId, guiRefId, fileName);

        try {
            // Create directories if not exist
            Files.createDirectories(targetPath.getParent());

            // Write dummy content to file
            String content = "Hello from File-Manager!";
            Files.write(targetPath, content.getBytes(StandardCharsets.UTF_8));

            System.out.println("✅ File successfully written at: " + targetPath);
        } catch (IOException e) {
            System.err.println("❌ Failed to write file to mount:");
            e.printStackTrace();
        }
    }
}
