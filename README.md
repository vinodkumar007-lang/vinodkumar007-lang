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

                    // 🔁 CALL OT API TO FETCH PROCESSED FILES
                    List<BatchFile> externalFiles = fetchProcessedFilesFromOpenText(kafkaMessage);
                    if (externalFiles != null && !externalFiles.isEmpty()) {
                        if (kafkaMessage.getBatchFiles() == null) {
                            kafkaMessage.setBatchFiles(new ArrayList<>());
                        }
                        kafkaMessage.getBatchFiles().addAll(externalFiles);
                    }

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

private List<BatchFile> fetchProcessedFilesFromOpenText(KafkaMessage kafkaMessage) {
        try {
            String url = "https://your-api.company.com/file-manager/opentext/processed";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<KafkaMessage> request = new HttpEntity<>(kafkaMessage, headers);
            ResponseEntity<BatchFile[]> response = restTemplate.postForEntity(url, request, BatchFile[].class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return Arrays.asList(response.getBody());
            }
        } catch (Exception e) {
            logger.error("❌ Error calling OpenText API: {}", e.getMessage(), e);
        }
        return Collections.emptyList();
    }
