/**
 * KafkaListenerService is responsible for:
 * - Listening to Kafka input topic for file batch processing messages
 * - Downloading blob files to local mount path
 * - Triggering Orchestration APIs (OT) like Debtman/MFC
 * - Waiting for generated output (STD XML), parsing error report and customer summaries
 * - Uploading processed files and generating summary.json
 * - Publishing final response message to Kafka output topic
 * 
 * This service acts as an orchestrator between Kafka, Blob Storage, OT system, and summary file generation.
 */
@Service
public class KafkaListenerService {

======

/**
 * Kafka consumer method to handle messages from input topic.
 * Performs validation on message structure, downloads files,
 * and triggers orchestration API.
 *
 * @param rawMessage Raw Kafka message in JSON string format
 * @param ack        Kafka acknowledgment to commit offset manually
 */
@KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
============

/**
 * Parses the STD XML file to extract customer delivery status information.
 *
 * @param xmlFile  STD Delivery XML file
 * @param errorMap ErrorReport map to determine status (SUCCESS/PARTIAL/FAILED)
 * @return List of CustomerSummary objects
 */
private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
===========
/**
 * Performs all post-orchestration processing like:
 * - Waiting for generated STD XML
 * - Parsing error report and STD XML
 * - Uploading output files and building processed files list
 * - Writing and uploading summary.json
 * - Sending Kafka output with summary response
 *
 * @param message     Kafka input message object
 * @param otResponse  OT job response containing jobId and id
 */
private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
===========
/**
 * Looks for a `.trigger` file in the output job directory and uploads it if found.
 *
 * @param jobDir  Path to the job output directory
 * @param message Kafka input message
 * @return Blob URL of uploaded trigger file or null if not found
 */
private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
=========
/**
 * Invokes the external OT orchestration batch API.
 *
 * @param token Authentication token
 * @param url   API endpoint
 * @param msg   Kafka input message payload
 * @return OTResponse containing jobId and id
 */
private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
=======
/**
 * Waits for the STD delivery XML file to be generated in job directory.
 * Ensures the file is stable (not still being written).
 *
 * @param jobId OT job ID
 * @param id    OT sub-job ID
 * @return File object for the found XML or null if timeout
 * @throws InterruptedException If thread sleep is interrupted
 */
private File waitForXmlFile(String jobId, String id) throws InterruptedException {
=======
/**
 * Parses ErrorReport.csv file under the job directory and maps delivery method status
 *
 * @param msg Kafka message to locate job folder
 * @return Map of accountNumber -> (method -> status)
 */
private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
=========
/**
 * Builds a list of SummaryProcessedFile entries with archive/output blob URLs
 * and delivery status for each customer (EMAIL, MOBSTAT, PRINT).
 *
 * @param jobDir        Output job directory
 * @param customerList  Customer basic summary list
 * @param errorMap      Error report map (account -> method -> status)
 * @param msg           Kafka message for path info
 * @return List of SummaryProcessedFile objects with blob URLs and status
 */
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {
==============
/**
 * Utility method to check if a string contains only digits.
 *
 * @param str Input string
 * @return true if numeric, false otherwise
 */
private boolean isNumeric(String str) {
===========
/**
 * Creates a deep copy of SummaryProcessedFile using BeanUtils.
 *
 * @param original Original SummaryProcessedFile
 * @return Copied instance
 */
private SummaryProcessedFile buildCopy(SummaryProcessedFile original) {
=========
/**
 * Uploads all files under the print directory and creates PrintFile entries.
 *
 * @param jobDir Output job directory
 * @param msg    Kafka message for blob path info
 * @return List of PrintFile objects containing blob URLs
 */
private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
========
/**
 * Extracts customer and page count values from STD XML's outputList node.
 *
 * @param xmlFile STD delivery XML
 * @return Map with "customersProcessed" and "pagesProcessed" as keys
 */
private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
=====
/**
 * Decodes a URL-encoded string using UTF-8 charset.
 *
 * @param url Encoded URL
 * @return Decoded URL string
 */
private String decodeUrl(String url) {
=======
/**
 * Gracefully shuts down executor service on bean destruction.
 */
@PreDestroy
public void shutdownExecutor() {
==========
/**
 * Internal class representing the response from OT orchestration call.
 */
static class OTResponse {
