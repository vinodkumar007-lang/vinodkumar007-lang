 Scenario 1 — Valid message (Only 1 DATA file — should process):
json
Copy
Edit
{
  "BatchId": "batch-data-1",
  "SourceSystem": "NDDSST",
  "TenantCode": "ZANBL",
  "ChannelID": null,
  "AudienceID": null,
  "Product": "NDDSST",
  "JobName": "NDDSST",
  "UniqueConsumerRef": "ref-data-1",
  "Timestamp": 1750338937031,
  "RunPriority": null,
  "EventType": null,
  "BatchFiles": [
    {
      "ObjectId": "idd-01",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST.dat",
      "Filename": "NDDSST.dat",
      "FileType": "DATA",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "true"
    }
  ]
}
❌ Scenario 2 — Invalid: Multiple DATA files (should reject):
json
Copy
Edit
{
  "BatchId": "batch-data-2",
  "SourceSystem": "NDDSST",
  "TenantCode": "ZANBL",
  "ChannelID": null,
  "AudienceID": null,
  "Product": "NDDSST",
  "JobName": "NDDSST",
  "UniqueConsumerRef": "ref-data-2",
  "Timestamp": 1750338937031,
  "RunPriority": null,
  "EventType": null,
  "BatchFiles": [
    {
      "ObjectId": "idd-01",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST.dat",
      "Filename": "NDDSST.dat",
      "FileType": "DATA",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "true"
    },
    {
      "ObjectId": "idd-02",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST2.dat",
      "Filename": "NDDSST2.dat",
      "FileType": "DATA",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "true"
    }
  ]
}
❌ Scenario 3 — Invalid: Only REF file (should reject):
json
Copy
Edit
{
  "BatchId": "batch-ref-1",
  "SourceSystem": "NDDSST",
  "TenantCode": "ZANBL",
  "ChannelID": null,
  "AudienceID": null,
  "Product": "NDDSST",
  "JobName": "NDDSST",
  "UniqueConsumerRef": "ref-ref-1",
  "Timestamp": 1750338937031,
  "RunPriority": null,
  "EventType": null,
  "BatchFiles": [
    {
      "ObjectId": "idd-01",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST.BR.dat",
      "Filename": "NDDSST.BR.dat",
      "FileType": "REF",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "false"
    }
  ]
}
❌ Scenario 4 — Invalid: REF + DATA mixed (should reject):
json
Copy
Edit
{
  "BatchId": "batch-mixed-1",
  "SourceSystem": "NDDSST",
  "TenantCode": "ZANBL",
  "ChannelID": null,
  "AudienceID": null,
  "Product": "NDDSST",
  "JobName": "NDDSST",
  "UniqueConsumerRef": "ref-mixed-1",
  "Timestamp": 1750338937031,
  "RunPriority": null,
  "EventType": null,
  "BatchFiles": [
    {
      "ObjectId": "idd-01",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST.BR.dat",
      "Filename": "NDDSST.BR.dat",
      "FileType": "REF",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "false"
    },
    {
      "ObjectId": "idd-02",
      "RepositoryId": "BATCH",
      "BlobUrl": "https://test.blob.core.windows.net/container/NDDSST.dat",
      "Filename": "NDDSST.dat",
      "FileType": "DATA",
      "ValidationStatus": "Valid",
      "ValidationRequirement": "true"
    }
  ]
}
❌ Scenario 5 — Invalid: No BatchFiles (empty array):
json
Copy
Edit
{
  "BatchId": "batch-empty-1",
  "SourceSystem": "NDDSST",
  "TenantCode": "ZANBL",
  "ChannelID": null,
  "AudienceID": null,
  "Product": "NDDSST",
  "JobName": "NDDSST",
  "UniqueConsumerRef": "ref-empty-1",
  "Timestamp": 1750338937031,
  "RunPriority": null,
  "EventType": null,
  "BatchFiles": []
}
