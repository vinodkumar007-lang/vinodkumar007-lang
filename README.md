import com.nedbank.kafka.filemanage.model.SummaryPayloadResponse;
import org.springframework.http.ResponseEntity;

@PostMapping("/process")
public ResponseEntity<SummaryPayloadResponse> triggerFileProcessing() {
    logger.info("POST /process called to trigger Kafka message processing.");
    try {
        return ResponseEntity.ok(kafkaListenerService.listen()); // returns SummaryPayloadResponse
    } catch (Exception e) {
        logger.error("Error during processing: ", e);
        return ResponseEntity.internalServerError().body(
            SummaryPayloadResponse.buildFailure("Processing failed: " + e.getMessage())
        );
    }
}
