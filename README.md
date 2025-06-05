package com.nedbank.kafka.filemanage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}

package com.nedbank.kafka.filemanage.controller;

import com.nedbank.kafka.filemanage.model.ApiResponse;
import com.nedbank.kafka.filemanage.model.SummaryPayloadResponse;
import com.nedbank.kafka.filemanage.service.KafkaListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<ApiResponse> triggerFileProcessing() {
        logger.info("POST /process called to trigger Kafka message processing.");
        try {
            return ResponseEntity.ok(kafkaListenerService.listen()); // returns SummaryPayloadResponse
        } catch (Exception e) {
            logger.error("Error during processing: ", e);
            return ResponseEntity.internalServerError().body(
                    ApiResponse.buildFailure("Processing failed: " + e.getMessage())
            );
        }
    }
}
