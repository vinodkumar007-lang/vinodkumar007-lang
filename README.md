curl --location --request POST 'https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService' \
--header 'mimeType: application/pdf' \
--header 'Authorization: Bearer eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiIxNDkzZWIwMy00NDY1LTQzODMtYTUyMy1mNzAyODVkM2QwNzQiLCJzYXQiOjE3NTA5NDY2MTIsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiIzZWMzNGY2YS1hYTc0LTRmNDYtYjQ2OC04OGM3OTEwYjA2MTkiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODI0ODI2MTIsImlhdCI6MTc1MDk0NjYxMiwianRpIjoiODY2ZmJiODItNTU2Ny00N2RlLWE1MmItOTg2OGFlZTM2OTZlIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.iqajBdAx-7snQv7ktQc-jh2vR0rblglCkkbEXHBSMnXnvILpjLtlgPpa3JCk6SwbKbBUTzBxz9-jJpVXztzwwtZajbYJJLy8OPENSVqVg1E8pq14GzS_jupu18xMw_sPIFxTLi9cfAvPLX9hJWnNoi0sZY19-JqakLC6kw-JgYTfD6WbTa-b1VsJroPwU4CmHN7eP7_YquzJF02MtjQsxpdjfMi9s4Un24MW3vHQcGNey2QoTsKM6P3zE6xXseFOoRIF1FKp8V-O316WtRVY50hBbgNG7J8ZQb4i9rg9vGUyIEcO0pnTrC2Lbj0YmWmBXQ5VwBTK1PcO7-79uz42iQ' \
--header 'Content-Type: application/json' \
--header 'Cookie: XSRF-TOKEN=c3abc48d-5c00-498c-b515-2486d4831574' \
--data-raw '{
    "BatchId": "1c93525b-42d1-410a-9e26-aa957f19861c",
    "SourceSystem": "DEBTMAN",
    "TenantCode": "ZANBL",
    "ChannelID": null,
    "AudienceID": null,
    "Product": "DEBTMAN",
    "JobName": "DEBTMAN",
    "UniqueConsumerRef": "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
    "Timestamp": 1748351245,
    "RunPriority": null,
    "EventType": null,
    "BatchFiles": [
        {
            "ObjectId": "{1037A096-0000-CE1A-A484-3290CA7938C4}",
            "RepositoryId": "BATCH",
            "BlobUrl": "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
            "Filename": "DEBTMAN.csv",
            "ValidationStatus": "valid",
            "ValidationRequirement": null
        }
    ]
}'
