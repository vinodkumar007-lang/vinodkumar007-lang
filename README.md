package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);

    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * Serializes the given SummaryPayload to a temporary file and returns the file path
     */
    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("❌ SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = payload.getBatchID() != null ? payload.getBatchID() : "unknown";
            String fileName = "summary_" + batchId + ".json";

            // Create temporary directory
            Path tempDir = Files.createTempDirectory("summaryFiles");

            Path summaryFilePath = tempDir.resolve(fileName);

            // Clean old file if exists
            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("⚠️ Existing summary file deleted before writing: {}", summaryFilePath);
            }

            // Write JSON to file
            objectMapper.writeValue(summaryFile, payload);

            logger.info("✅ Summary JSON successfully written: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    /**
     * Builds a basic payload wrapper if you want to build one from response
     */
    public static SummaryPayload buildPayload(KafkaMessage message, java.util.List<SummaryProcessedFile> processedFiles) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + "_summary.json");
        payload.setProcessedFiles(processedFiles);
        return payload;
    }
}
====================
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${opentext.api.url}")
    private String opentextApiUrl;

    @Value("${otds.token.url}")
    private String otdsTokenUrl;

    @Value("${otds.username}")
    private String otdsUsername;

    @Value("${otds.password}")
    private String otdsPassword;

    @Value("${otds.client-id}")
    private String otdsClientId;

    @Value("${otds.client-secret}")
    private String otdsClientSecret;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService,
                                KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("📩 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("✅ Sent processed response to Kafka output topic.");
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            String batchId = message.getBatchId();
            String guiRef = message.getUniqueConsumerRef();
            Path jobDir = Paths.get(mountPath, batchId, guiRef);
            Files.createDirectories(jobDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String fileName = extractFileName(blobUrl);
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = jobDir.resolve(fileName);
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
                logger.info("✅ Mounted file to: {}", localPath);
            }

            // Generate metadata.json
            writeAndUploadMetadataJson(message, jobDir);

            // Send to OT
            String token = fetchAccessToken();
            sendToOpenText(token, message);

            // Wait for .rpt file
            File rptFile = waitForRptFile(jobDir);
            if (rptFile == null) {
                logger.error("⏰ Timeout waiting for .rpt file in {}", jobDir);
                return new ApiResponse("Timeout waiting for .rpt", "error", null);
            }

            logger.info("📄 Found .rpt file: {}", rptFile.getAbsolutePath());

            // Read account numbers from .rpt
            Set<String> accountNumbers = extractAccountNumbersFromRpt(rptFile);

            // Upload matching output files from mount
            List<SummaryProcessedFile> processedFiles = new ArrayList<>();
            for (String acc : accountNumbers) {
                File[] matching = jobDir.toFile().listFiles(f -> f.getName().startsWith(acc));
                if (matching == null) continue;

                for (File f : matching) {
                    String blobPath = String.format("output/%s/%s", batchId, f.getName());
                    String uploadedUrl = blobStorageService.uploadFile(f.getAbsolutePath(), blobPath);
                    SummaryProcessedFile pf = new SummaryProcessedFile();
                    pf.setAccountNumber(acc);
                    pf.setBlobURL(decodeUrl(uploadedUrl));
                    pf.setStatusCode("OK");
                    pf.setStatusDescription("Uploaded");
                    processedFiles.add(pf);
                    FileUtils.forceDelete(f);
                }
            }

            // Upload summary.json
            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            SummaryResponse summaryResponse = new SummaryResponse(payload);
            return new ApiResponse("Success", "success", summaryResponse);
        } catch (Exception ex) {
            logger.error("❌ Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private void writeAndUploadMetadataJson(KafkaMessage message, Path jobDir) {
        try {
            Map<String, Object> metaMap = objectMapper.convertValue(message, Map.class);
            if (metaMap.containsKey("batchFiles")) {
                List<Map<String, Object>> files = (List<Map<String, Object>>) metaMap.get("batchFiles");
                for (Map<String, Object> f : files) {
                    Object blob = f.remove("blobUrl");
                    if (blob != null) f.put("fileLocation", blob);
                }
            }
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap);
            File metaFile = new File(jobDir.toFile(), "metadata.json");
            FileUtils.writeStringToFile(metaFile, json, StandardCharsets.UTF_8);
            String blobPath = String.format("%s/Trigger/metadata_%s.json", message.getSourceSystem(), message.getBatchId());
            blobStorageService.uploadFile(metaFile.getAbsolutePath(), blobPath);
            logger.info("✅ metadata.json uploaded to {}", blobPath);
            FileUtils.forceDelete(metaFile);
        } catch (Exception ex) {
            logger.error("❌ metadata.json generation failed", ex);
        }
    }

    private String fetchAccessToken() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
            body.add("grant_type", "password");
            body.add("username", otdsUsername);
            body.add("password", otdsPassword);
            body.add("client_id", otdsClientId);
            body.add("client_secret", otdsClientSecret);

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<Map> response = restTemplate.postForEntity(otdsTokenUrl, request, Map.class);
            return (String) response.getBody().get("access_token");
        } catch (Exception e) {
            logger.error("❌ Failed to get OTDS token", e);
            throw new RuntimeException("OTDS auth failed");
        }
    }

    private void sendToOpenText(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);
            String json = objectMapper.writeValueAsString(msg);
            HttpEntity<String> request = new HttpEntity<>(json, headers);
            restTemplate.postForEntity(opentextApiUrl, request, String.class);
            logger.info("📤 Sent metadata to OT");
        } catch (Exception ex) {
            logger.error("❌ Failed to send data to OT", ex);
            throw new RuntimeException("OT call failed");
        }
    }

    private File waitForRptFile(Path jobDir) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < rptMaxWaitSeconds * 1000L) {
            File[] rptFiles = jobDir.toFile().listFiles(f -> f.getName().endsWith(".rpt"));
            if (rptFiles != null && rptFiles.length > 0) {
                return rptFiles[0];
            }
            try {
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            } catch (InterruptedException ignored) {
            }
        }
        return null;
    }

    private Set<String> extractAccountNumbersFromRpt(File rptFile) throws IOException {
        Set<String> accNums = new HashSet<>();
        Pattern accPattern = Pattern.compile("\\b\\d{9,}\\b"); // e.g., 9+ digit number
        try (BufferedReader reader = new BufferedReader(new FileReader(rptFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = accPattern.matcher(line);
                while (m.find()) {
                    accNums.add(m.group());
                }
            }
        }
        return accNums;
    }

    private String extractFileName(String url) {
        try {
            String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
            return Paths.get(new URI(decoded).getPath()).getFileName().toString();
        } catch (Exception e) {
            logger.warn("⚠️ Failed to extract file name, fallback to last segment: {}", url);
            String[] parts = url.split("/");
            return parts[parts.length - 1];
        }
    }

    private String decodeUrl(String encodedUrl) {
        try {
            return URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return encodedUrl;
        }
    }
}
===================
mount.path=/mnt/nfs/dev-exstream/dev-SA/jobs
opentext.api.url=http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService
otds.token.url=https://dev-exstream.nednet.co.za/otds/otdstenant/dev-exstream/otdsws/login
otds.username=tenantadmin
otds.password=Exstream1!
otds.client-id=devexstreamclient
otds.client-secret=nV6A23zcFU5bK6lwm5KLBY2r5Sm3bh5l
rpt.max.wait.seconds=120
rpt.poll.interval.millis=5000
====================
package com.nedbank.kafka.filemanage.service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.nedbank.kafka.filemanage.exception.CustomAppException;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@Service
public class BlobStorageService {

    private static final Logger logger = LoggerFactory.getLogger(BlobStorageService.class);

    private final RestTemplate restTemplate;

    @Value("${azure.keyvault.url}")
    private String keyVaultUrl;

    @Value("${azure.blob.storage.format}")
    private String azureStorageFormat;

    @Value("${azure.keyvault.accountKey}")
    private String fmAccountKey;

    @Value("${azure.keyvault.accountName}")
    private String fmAccountName;

    @Value("${azure.keyvault.containerName}")
    private String fmContainerName;

    private String accountKey;
    private String accountName;
    private String containerName;

    public BlobStorageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    private void initSecrets() {
        if (accountKey != null && accountName != null && containerName != null) return;

        try {
            logger.info("🔐 Fetching secrets from Azure Key Vault...");
            SecretClient secretClient = new SecretClientBuilder()
                    .vaultUrl(keyVaultUrl)
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .buildClient();

            accountKey = getSecret(secretClient, fmAccountKey);
            accountName = getSecret(secretClient, fmAccountName);
            containerName = getSecret(secretClient, fmContainerName);

            if (accountKey == null || accountName == null || containerName == null) {
                throw new CustomAppException("Secrets missing from Key Vault", 400, HttpStatus.BAD_REQUEST);
            }

            logger.info("✅ Secrets fetched successfully from Key Vault.");
        } catch (Exception e) {
            logger.error("❌ Failed to initialize secrets: {}", e.getMessage(), e);
            throw new CustomAppException("Key Vault integration failure", 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private String getSecret(SecretClient client, String secretName) {
        try {
            return client.getSecret(secretName).getValue();
        } catch (Exception e) {
            logger.error("❌ Failed to fetch secret '{}': {}", secretName, e.getMessage(), e);
            throw new CustomAppException("Failed to fetch secret: " + secretName, 500, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String copyFileFromUrlToBlob(String sourceUrl, String targetBlobPath) {
        try {
            initSecrets();
            URI uri = new URI(sourceUrl);
            String[] parts = uri.getPath().split("/", 3);
            if (parts.length < 3) throw new CustomAppException("Invalid source URL", 400, HttpStatus.BAD_REQUEST);

            String sourceContainer = parts[1];
            String blobPath = parts[2];
            String sourceAccount = uri.getHost().split("\\.")[0];

            BlobServiceClient srcClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, sourceAccount))
                    .credential(new StorageSharedKeyCredential(sourceAccount, accountKey))
                    .buildClient();

            BlobContainerClient srcContainer = srcClient.getBlobContainerClient(sourceContainer);
            BlobClient srcBlob = srcContainer.getBlobClient(blobPath);

            if (!srcBlob.exists()) throw new CustomAppException("Source blob not found", 404, HttpStatus.NOT_FOUND);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            srcBlob.download(out);
            byte[] data = out.toByteArray();

            BlobServiceClient tgtClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobContainerClient tgtContainer = tgtClient.getBlobContainerClient(containerName);
            BlobClient tgtBlob = tgtContainer.getBlobClient(targetBlobPath);
            tgtBlob.upload(new ByteArrayInputStream(data), data.length, true);

            logger.info("✅ Copied '{}' to '{}'", sourceUrl, tgtBlob.getBlobUrl());
            return tgtBlob.getBlobUrl();

        } catch (Exception e) {
            logger.error("❌ Error copying blob: {}", e.getMessage(), e);
            throw new CustomAppException("Error copying blob", 601, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String uploadFile(String content, String targetPath) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            blob.upload(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), content.length(), true);

            logger.info("✅ Uploaded file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("❌ Upload failed: {}", e.getMessage(), e);
            throw new CustomAppException("Upload failed", 602, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String downloadFileContent(String blobPathOrUrl) {
        try {
            initSecrets();
            String container = containerName;
            String blobPath = blobPathOrUrl;

            if (blobPathOrUrl.startsWith("http")) {
                URI uri = new URI(blobPathOrUrl);
                String[] segments = uri.getPath().split("/");
                if (segments.length < 3) throw new CustomAppException("Invalid blob URL", 400, HttpStatus.BAD_REQUEST);
                container = segments[1];
                blobPath = String.join("/", Arrays.copyOfRange(segments, 2, segments.length));
            }

            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(container).getBlobClient(blobPath);
            if (!blob.exists()) throw new CustomAppException("Blob not found", 404, HttpStatus.NOT_FOUND);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            blob.download(out);
            return out.toString(StandardCharsets.UTF_8);

        } catch (Exception e) {
            logger.error("❌ Download failed: {}", e.getMessage(), e);
            throw new CustomAppException("Download failed", 603, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    public String buildPrintFileUrl(KafkaMessage message) {
        initSecrets();

        String dateFolder = Instant.ofEpochMilli(message.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        String printFileName = message.getBatchId() + "_printfile.pdf";

        return String.format("%s/%s/%s/%s/%s/%s/print/%s",
                String.format(azureStorageFormat, accountName, containerName),
                message.getSourceSystem(),
                dateFolder,
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                message.getJobName(),
                printFileName);
    }

    public String uploadSummaryJson(String filePathOrUrl, KafkaMessage message, String fileName) {
        initSecrets();

        String remotePath = String.format("%s/%s/%s/%s",
                message.getSourceSystem(),
                message.getBatchId(),
                message.getUniqueConsumerRef(),
                fileName);

        try {
            String json = filePathOrUrl.startsWith("http")
                    ? new String(new URL(filePathOrUrl).openStream().readAllBytes(), StandardCharsets.UTF_8)
                    : Files.readString(Paths.get(filePathOrUrl));

            return uploadFile(json, remotePath);
        } catch (Exception e) {
            logger.error("❌ Failed reading summary JSON: {}", e.getMessage(), e);
            throw new CustomAppException("Failed reading summary JSON", 604, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
