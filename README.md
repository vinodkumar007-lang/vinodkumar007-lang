package com.nedbank.kafka.filemanage.controller;

import com.nedbank.kafka.filemanage.service.KafkaListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/file")
public class FileProcessingController {

    private static final Logger logger = LoggerFactory.getLogger(FileProcessingController.class);

    @Autowired
    private KafkaListenerService kafkaListenerService;

    // Endpoint to manually trigger the Kafka message processing
    @PostMapping("/process")
    public String processFile(@RequestBody String message) {
        logger.info("Received request to process file with message: {}", message);

        try {
            // Manually consume the message, upload the file, and send URL to Kafka
            kafkaListenerService.consumeMessageAndStoreFile(message);
            logger.info("File processed successfully for message: {}", message);
            return "File processing triggered successfully!";
        } catch (Exception e) {
            logger.error("Error processing file for message: {}. Error: {}", message, e.getMessage());
            return "Error processing file: " + e.getMessage();
        }
    }
}

