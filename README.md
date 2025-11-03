import lombok.*;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ECPBatchAudit {

    private String title;
    private String type;
    private Properties properties;
    private boolean success;
    private String errorCode;
    private String errorMessage;
    private boolean retryFlag;
    private int retryCount;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Properties {
        private String datastreamName;
        private String datastreamType;
        private String batchId;
        private String serviceName;
        private String systemEnv;
        private String sourceSystem;
        private String tenantCode;
        private String channelId;
        private String audienceId;
        private String product;
        private String jobName;
        private String consumerRef;
        private String timestamp;
        private String eventType;
        private Instant startTime;
        private Instant endTime;
        private long customerCount;
        private List<BatchFileAudit> batchFiles;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchFileAudit {
        private String type;
        private FileProperties properties;

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class FileProperties {
            private String blobUrl;
            private String fileName;
            private String fileType;
        }
    }
}
