package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DataParser {
    private static final Logger logger = LoggerFactory.getLogger(DataParser.class);

    /**
     * Parses raw input file content to extract a list of CustomerData objects.
     * Assumes each line is formatted as:
     * customerId,accountNumber,name,email,deliveryChannel,mobileNumber,printIndicator,<optional extra fields>
     */
    public static List<CustomerData> extractCustomerData(String content, String deliveryType) {
        List<CustomerData> customers = new ArrayList<>();
        String[] lines = content.split("\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] fields = line.split("\\|", -1);  // Use -1 to preserve trailing empty fields
            if (fields.length < 5) {
                logger.warn("Skipping line due to insufficient fields: {}", line);
                continue;
            }

            // Only process lines starting with "05"
            if ("05".equals(fields[0])) {
                // Check delivery type is in 5th field (index 4)
                if (deliveryType.equalsIgnoreCase(fields[4])) {
                    CustomerData customer = new CustomerData();
                    // Example mappings - adjust as per your Customer class
                    customer.setCustomerId(fields[1]);
                    customer.setAccountNumber(fields[2]);
                    customer.setTenantCode(fields[3]);
                    customer.setDeliveryChannel(fields[4]);
                    // Add more fields as needed

                    customers.add(customer);
                    logger.info("Extracted customer: {}", customer);
                }
            }
        }
        return customers;
    }

}

package com.nedbank.kafka.filemanage.service;

import com.nedbank.kafka.filemanage.model.CustomerData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileGenerator {

    /**
     * Generates a simple PDF file for a customer (dummy placeholder).
     * You might want to replace this with actual PDF library usage (e.g., iText or Apache PDFBox).
     */
    public static File generatePdf(CustomerData customer) throws IOException {
        File pdfFile = File.createTempFile(customer.getCustomerId() + "_", ".pdf");

        try (FileWriter writer = new FileWriter(pdfFile)) {
            writer.write("PDF content for customer:\n");
            writer.write("Customer ID: " + customer.getCustomerId() + "\n");
            writer.write("Account Number: " + customer.getAccountNumber() + "\n");
            writer.write("Name: " + customer.getName() + "\n");
        }

        return pdfFile;
    }

    /**
     * Generates a simple HTML file for a customer.
     */
    public static File generateHtml(CustomerData customer) throws IOException {
        File htmlFile = File.createTempFile(customer.getCustomerId() + "_", ".html");

        try (FileWriter writer = new FileWriter(htmlFile)) {
            writer.write("<html><body>");
            writer.write("<h1>Customer Report</h1>");
            writer.write("<p><strong>Customer ID:</strong> " + customer.getCustomerId() + "</p>");
            writer.write("<p><strong>Account Number:</strong> " + customer.getAccountNumber() + "</p>");
            writer.write("<p><strong>Name:</strong> " + customer.getName() + "</p>");
            writer.write("</body></html>");
        }

        return htmlFile;
    }

    /**
     * Generates a simple TXT file for a customer.
     */
    public static File generateTxt(CustomerData customer) throws IOException {
        File txtFile = File.createTempFile(customer.getCustomerId() + "_", ".txt");

        try (FileWriter writer = new FileWriter(txtFile)) {
            writer.write("Customer Report\n");
            writer.write("Customer ID: " + customer.getCustomerId() + "\n");
            writer.write("Account Number: " + customer.getAccountNumber() + "\n");
            writer.write("Name: " + customer.getName() + "\n");
        }

        return txtFile;
    }

    /**
     * Generates a simple MOBSTAT file for a customer.
     * (Format as needed for MOBSTAT system)
     */
    public static File generateMobstat(CustomerData customer) throws IOException {
        File mobstatFile = File.createTempFile(customer.getCustomerId() + "_", ".mobstat");

        try (FileWriter writer = new FileWriter(mobstatFile)) {
            // Example mobstat content format
            writer.write("MOBSTAT Report\n");
            writer.write("ID:" + customer.getCustomerId() + "\n");
            writer.write("ACC:" + customer.getAccountNumber() + "\n");
            writer.write("NAME:" + customer.getName() + "\n");
        }

        return mobstatFile;
    }
}
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    public ApiResponse listen() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");
        props.put("group.id", "str-ecp-batch");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
        props.put("ssl.truststore.password", "nedbank1");
        props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
        props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put("ssl.protocol", "TLSv1.2");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition(inputTopic, 0);
            consumer.assign(Collections.singletonList(partition));

            OffsetAndMetadata committed = consumer.committed(partition);
            long nextOffset = committed != null ? committed.offset() : 0;

            consumer.seek(partition, nextOffset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages at offset {}", nextOffset);
                return new ApiResponse(
                        "No new messages to process",
                        "info",
                        new SummaryPayloadResponse("No new messages to process", "info", new SummaryResponse()).getSummaryResponse()
                );
            }

            for (ConsumerRecord<String, String> record : records) {
                try {
                    KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(kafkaMessage);
                    kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));
                    consumer.commitSync(Collections.singletonMap(
                            partition,
                            new OffsetAndMetadata(record.offset() + 1)
                    ));
                    return response;
                } catch (Exception ex) {
                    logger.error("Error processing Kafka message", ex);
                    return new ApiResponse(
                            "Error processing message: " + ex.getMessage(),
                            "error",
                            new SummaryPayloadResponse("Error processing message", "error", new SummaryResponse()).getSummaryResponse()
                    );
                }
            }
        } catch (Exception e) {
            logger.error("Kafka consumer failed", e);
            return new ApiResponse(
                    "Kafka error: " + e.getMessage(),
                    "error",
                    new SummaryPayloadResponse("Kafka error", "error", new SummaryResponse()).getSummaryResponse()
            );
        }

        return new ApiResponse(
                "No messages processed",
                "info",
                new SummaryPayloadResponse("No messages processed", "info", new SummaryResponse()).getSummaryResponse()
        );
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        if (message == null) {
            return new ApiResponse("Empty message", "error",
                    new SummaryPayloadResponse("Empty message", "error", new SummaryResponse()).getSummaryResponse());
        }

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setTimestamp(instantToIsoString(message.getTimestamp()));
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        Payload payload = new Payload();
        payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payload.setRunPriority(message.getRunPriority());
        payload.setEventType(message.getEventType());

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();
        Metadata metadata = new Metadata();
        String summaryFileUrl;
        int fileCount = 0;

        // <-- HERE is the updated fileName extraction:
        String fileName = null;
        if (message.getBatchFiles() != null && !message.getBatchFiles().isEmpty()) {
            String firstBlobUrl = message.getBatchFiles().get(0).getBlobUrl();
            String blobPath = extractBlobPath(firstBlobUrl);
            fileName = extractFileName(blobPath);
        }
        if (fileName == null || fileName.isEmpty()) {
            fileName = message.getBatchId() + ".json";  // fallback if no file name found
        }

        for (BatchFile file : message.getBatchFiles()) {
            try {
                logger.debug("Processing batch file: {}", file.getFilename());

                String sourceBlobUrl = file.getBlobUrl();
                String resolvedBlobPath = extractBlobPath(sourceBlobUrl);
                String sanitizedBlobName = extractFileName(resolvedBlobPath);
                String inputFileContent = blobStorageService.downloadFileContent(sanitizedBlobName);

                logger.debug("Downloaded file content length for {}: {}", sanitizedBlobName,
                        inputFileContent != null ? inputFileContent.length() : "null");

                assert inputFileContent != null;
                List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent, "PRINT");

                if (customers.isEmpty()) {
                    logger.warn("No customers extracted from file {}", file.getFilename());
                    continue;
                }

                logger.debug("Extracted {} customers from file {}", customers.size(), file.getFilename());

                for (CustomerData customer : customers) {
                    logger.debug("Processing customer ID: {}", customer.getCustomerId());

                    File pdfFile = FileGenerator.generatePdf(customer);
                    File htmlFile = FileGenerator.generateHtml(customer);
                    File txtFile = FileGenerator.generateTxt(customer);
                    File mobstatFile = FileGenerator.generateMobstat(customer);

                    String pdfArchiveBlobPath = buildBlobPath(
                            message.getSourceSystem(),
                            message.getTimestamp(),
                            message.getBatchId(),
                            message.getUniqueConsumerRef(),
                            message.getJobName(),
                            "archive",
                            customer.getAccountNumber(),
                            pdfFile.getName());

                    String pdfEmailBlobPath = buildBlobPath(
                            message.getSourceSystem(),
                            message.getTimestamp(),
                            message.getBatchId(),
                            message.getUniqueConsumerRef(),
                            message.getJobName(),
                            "email",
                            customer.getAccountNumber(),
                            pdfFile.getName());

                    String htmlEmailBlobPath = buildBlobPath(
                            message.getSourceSystem(),
                            message.getTimestamp(),
                            message.getBatchId(),
                            message.getUniqueConsumerRef(),
                            message.getJobName(),
                            "html",
                            customer.getAccountNumber(),
                            htmlFile.getName());

                    String txtEmailBlobPath = buildBlobPath(
                            message.getSourceSystem(),
                            message.getTimestamp(),
                            message.getBatchId(),
                            message.getUniqueConsumerRef(),
                            message.getJobName(),
                            "txt",
                            customer.getAccountNumber(),
                            txtFile.getName());

                    String mobstatBlobPath = buildBlobPath(
                            message.getSourceSystem(),
                            message.getTimestamp(),
                            message.getBatchId(),
                            message.getUniqueConsumerRef(),
                            message.getJobName(),
                            "mobstat",
                            customer.getAccountNumber(),
                            mobstatFile.getName());

                    String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfArchiveBlobPath);
                    String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(), pdfEmailBlobPath);
                    String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(), htmlEmailBlobPath);
                    String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(), txtEmailBlobPath);
                    String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(), mobstatBlobPath);

                    if (pdfArchiveUrl == null || pdfArchiveUrl.isBlank()) {
                        logger.warn("pdfArchiveUrl is empty for customerId: {}", customer.getCustomerId());
                    }
                    if (pdfEmailUrl == null || pdfEmailUrl.isBlank()) {
                        logger.warn("pdfEmailUrl is empty for customerId: {}", customer.getCustomerId());
                    }
                    if (htmlEmailUrl == null || htmlEmailUrl.isBlank()) {
                        logger.warn("htmlEmailUrl is empty for customerId: {}", customer.getCustomerId());
                    }
                    if (txtEmailUrl == null || txtEmailUrl.isBlank()) {
                        logger.warn("txtEmailUrl is empty for customerId: {}", customer.getCustomerId());
                    }
                    if (mobstatUrl == null || mobstatUrl.isBlank()) {
                        logger.warn("mobstatUrl is empty for customerId: {}", customer.getCustomerId());
                    }

                    SummaryProcessedFile processedFile = new SummaryProcessedFile();
                    processedFile.setCustomerID(customer.getCustomerId());
                    processedFile.setAccountNumber(customer.getAccountNumber());
                    processedFile.setPdfArchiveFileURL(pdfArchiveUrl);
                    processedFile.setPdfEmailFileURL(pdfEmailUrl);
                    processedFile.setHtmlEmailFileURL(htmlEmailUrl);
                    processedFile.setTxtEmailFileURL(txtEmailUrl);
                    processedFile.setPdfMobstatFileURL(mobstatUrl);
                    processedFile.setStatusCode("OK");
                    processedFile.setStatusDescription("Success");

                    processedFiles.add(processedFile);
                    fileCount++;
                    logger.debug("Added processed file for customer ID: {}", customer.getCustomerId());
                }
            } catch (Exception ex) {
                logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
            }
        }

        logger.info("Total processed files count: {}", processedFiles.size());

        PrintFile printFile = new PrintFile();
        printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
        printFiles.add(printFile);

        metadata.setProcessingStatus("Completed");
        metadata.setTotalFilesProcessed(processedFiles.size());
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName(fileName);
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);
        logger.debug("Summary JSON file path: {}", summaryJsonPath);
        summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        SummaryResponse summaryResponse = new SummaryResponse();
        summaryResponse.setBatchID(summaryPayload.getBatchID());
        summaryResponse.setFileName(summaryPayload.getFileName());
        summaryResponse.setHeader(summaryPayload.getHeader());
        summaryResponse.setMetadata(summaryPayload.getMetadata());
        summaryResponse.setPayload(summaryPayload.getPayload());
        summaryResponse.setSummaryFileURL(summaryPayload.getSummaryFileURL());
        summaryResponse.setTimestamp(String.valueOf(Instant.now()));

        SummaryPayloadResponse apiPayload = new SummaryPayloadResponse("Batch processed successfully", "success", summaryResponse);

        return new ApiResponse(apiPayload.getMessage(), apiPayload.getStatus(), apiPayload.getSummaryResponse());
    }

    private String buildBlobPath(String sourceSystem, long timestamp, String batchId,
                                 String uniqueConsumerRef, String jobName, String folder,
                                 String customerAccount, String fileName) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());
        String dateStr = dtf.format(Instant.ofEpochMilli(timestamp));
        return String.format("%s/%s/%s/%s/%s/%s/%s",
                sourceSystem, dateStr, batchId, uniqueConsumerRef, jobName, folder, fileName);
    }

    private String extractBlobPath(String fullUrl) {
        if (fullUrl == null) return "";
        try {
            URI uri = URI.create(fullUrl);
            String path = uri.getPath();
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            return path;
        } catch (Exception e) {
            return fullUrl;
        }
    }

    public String extractFileName(String fullPathOrUrl) {
        if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) {
            return fullPathOrUrl;
        }
        String trimmed = fullPathOrUrl.replaceAll("/+", "/");
        int lastSlashIndex = trimmed.lastIndexOf('/');
        return lastSlashIndex >= 0 ? trimmed.substring(lastSlashIndex + 1) : trimmed;
    }

    private String instantToIsoString(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }
}

