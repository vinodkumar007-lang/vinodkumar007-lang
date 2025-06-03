package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
    public static String writeSummaryJson(SummaryPayload payload) {
        try {
            StringWriter writer = new StringWriter();
            objectMapper.writeValue(writer, payload);
            logger.info("✅ Summary JSON successfully written.");
            return writer.toString();
        } catch (Exception e) {
            logger.error("❌ Failed to write summary JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }
}
