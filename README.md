package com.nedbank.kafka.filemanage.controller;

import com.nedbank.kafka.filemanage.service.KafkaListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/file")
public class FileProcessingController {

    private static final Logger logger = LoggerFactory.getLogger(FileProcessingController.class);
    private final KafkaListenerService kafkaListenerService;

    public FileProcessingController(KafkaListenerService kafkaListenerService) {
        this.kafkaListenerService = kafkaListenerService;
    }

    // Health check
    @GetMapping("/health")
    public String healthCheck() {
        logger.info("Health check endpoint hit.");
        return "File Processing Service is up and running.";
    }

    @PostMapping("/process")
    public Map<String, Object> triggerFileProcessing() {
        logger.info("POST /process called to trigger Kafka message processing.");
        return kafkaListenerService.listen();
    }
}

method listen in class com.nedbank.kafka.filemanage.service.KafkaListenerService cannot be applied to given types;
