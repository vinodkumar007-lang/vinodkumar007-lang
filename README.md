package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.nio.file.Path;
import java.nio.file.Files;


import java.io.StringWriter;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public SummaryJsonWriter() {
        objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // pretty print
    }

    /**
     * Serializes the given SummaryPayload to JSON string
     */
    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        try {
            // Prepare clean filename
            String fileName = "summary_" + payload.getBatchID() + ".json";

            // Create a temp directory to store summary files (optional, you can change folder)
            Path tempDir = Files.createTempDirectory("summaryFiles");

            // Full path with clean filename
            Path filePath = tempDir.resolve(fileName);

            // Write JSON to file
            objectMapper.writeValue(filePath.toFile(), payload);

            logger.info("✅ Summary JSON successfully written to file: {}", filePath);
            return filePath.toString();
        } catch (Exception e) {
            logger.error("❌ Failed to write summary JSON to file: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON to file", e);
        }
    }
}
