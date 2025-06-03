package com.nedbank.kafka.filemanage.util;

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
    private final ObjectMapper objectMapper;

    public SummaryJsonWriter() {
        this.objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT); // pretty print
    }

    /**
     * Serializes the given SummaryPayload to JSON string
     */
    public String writeSummaryJson(SummaryPayload payload) {
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
