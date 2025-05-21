private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
    List<ProcessedFileInfo> processedFiles = new ArrayList<>();
    Set<String> filenetObjectIds = new HashSet<>();
    Date now = new Date();

    // === Populate Processed Files ===
    for (JsonNode fileNode : batchFilesNode) {
        String objectId = fileNode.get("ObjectId").asText();
        String fileLocation = fileNode.get("fileLocation").asText();
        String customerId = objectId.split("_")[0];
        String pdfUrl = "file://" + fileLocation;

        processedFiles.add(new ProcessedFileInfo(customerId, pdfUrl));
        filenetObjectIds.add(objectId);
    }

    // === Build Header ===
    HeaderInfo header = new HeaderInfo();
    header.setTenantCode(extractField(batchFilesNode.get(0), "tenantCode"));
    header.setChannelID(extractField(batchFilesNode.get(0), "channelID"));
    header.setAudienceID(extractField(batchFilesNode.get(0), "audienceID"));
    header.setTimestamp(now.toInstant().toString());
    header.setSourceSystem(extractField(batchFilesNode.get(0), "sourceSystem"));
    header.setProduct(extractField(batchFilesNode.get(0), "product"));
    header.setJobName(extractField(batchFilesNode.get(0), "jobName"));

    // === Build Metadata ===
    MetadataInfo metadata = new MetadataInfo();
    metadata.setTotalFilesProcessed(batchFilesNode.size());
    metadata.setProcessingStatus("Success");
    metadata.setEventOutcomeCode("Success");
    metadata.setEventOutcomeDescription("All customer PDFs processed successfully");

    // === Build Payload ===
    PayloadInfo payload = new PayloadInfo();
    payload.setUniqueConsumerRef(extractField(batchFilesNode.get(0), "consumerReference"));
    payload.setUniqueECPBatchRef(batchId);
    payload.setFilenetObjectID(new ArrayList<>(filenetObjectIds));
    payload.setRepositoryID("Legacy");
    payload.setRunPriority("High");
    payload.setEventID("E" + batchId);
    payload.setEventType("Completion");
    payload.setRestartKey("Key" + batchId);

    // === Build SummaryPayload ===
    SummaryPayload summary = new SummaryPayload();
    summary.setBatchID(batchId);
    summary.setHeader(header);
    summary.setMetadata(metadata);
    summary.setPayload(payload);
    summary.setProcessedFiles(processedFiles);
    summary.setSummaryFileURL("file://summary_outputs/summary_" + batchId + ".json");
    summary.setTimestamp(now.toInstant().toString());

    // === Save to local file (optional, if not already done elsewhere) ===
    try {
        String userHome = System.getProperty("user.home");
        File outputDir = new File(userHome, "summary_outputs");
        if (!outputDir.exists()) outputDir.mkdirs();

        String summaryPath = new File(outputDir, "summary_" + batchId + ".json").getAbsolutePath();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(summaryPath), summary);
        logger.info("Saved local summary to {}", summaryPath);
    } catch (Exception e) {
        logger.error("Failed to write summary file", e);
    }

    return objectMapper.convertValue(summary, Map.class);
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.List;

@Data
public class SummaryPayload {
    private String BatchID;
    private HeaderInfo Header;
    private MetadataInfo Metadata;
    private PayloadInfo Payload;
    private List<ProcessedFileInfo> ProcessedFiles;
    private String SummaryFileURL;
    private String Timestamp;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class HeaderInfo {
    private String TenantCode;
    private String ChannelID;
    private String AudienceID;
    private String Timestamp;
    private String SourceSystem;
    private String Product;
    private String JobName;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;

@Data
public class MetadataInfo {
    private int TotalFilesProcessed;
    private String ProcessingStatus;
    private String EventOutcomeCode;
    private String EventOutcomeDescription;
}
package com.nedbank.kafka.filemanage.model;

import lombok.Data;
import java.util.List;

@Data
public class PayloadInfo {
    private String UniqueConsumerRef;
    private String UniqueECPBatchRef;
    private List<String> FilenetObjectID;
    private String RepositoryID;
    private String RunPriority;
    private String EventID;
    private String EventType;
    private String RestartKey;
}
package com.nedbank.kafka.filemanage.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedFileInfo {
    private String CustomerID;
    private String PDFFileURL;
}
@JsonProperty("BatchID")
private String batchId;
