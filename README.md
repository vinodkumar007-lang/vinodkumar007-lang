SummaryPayloadResponse response = new SummaryPayloadResponse();
response.setMessage("Batch processed successfully");
response.setStatus("success");

SummaryPayload payload = new SummaryPayload();
payload.setBatchID(batchID);
payload.setHeader(header);
payload.setMetadata(metadata);
payload.setPayload(payloadData);
payload.setSummaryFileURL(summaryFileBlobUrl);
payload.setTimestamp(Instant.now().toString());

response.setSummaryPayload(payload);
return response;
