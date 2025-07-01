curl --location --request GET 'http://exstream-deployment-orchestration-service.dev-exstream.svc:8900/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService' \
--header 'Authorization: Bearer eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiJiZjQxOWRiNi03OTlhLTQ4ZTAtYjhmYy01ZTFiMWQ3ODYxYmMiLCJzYXQiOjE3NDk4MDY2MjAsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiI3MDNjYTEyYy1kNDdlLTRmOGYtOWY0OS05OWM5YWI3OWNjMDIiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODEzNDI2MjAsImlhdCI6MTc0OTgwNjYyMCwianRpIjoiOTA3YmQzMjItNDczMi00NDA0LWJiMTUtOGI5MjI1MWZiZjQ0IiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JIFEiABAISjp1uPQo-ubp4xUpxp67W4z_ynAOYywPkazTMFfniz-Tojb0uWGEilrbebIuljvjmgNfOOnrInalkaYu9-V6M4yCEWsPXJHcRB6HsqywXCgq4wB0fHGT5yCG7C9ggjNQXfSo6VPSQ5TFBaBdiFJ5H52QOwUL3rxCEkfIJx7LZDVu5Q2uEP2xRyj3dlw9kE-0cXX2cs1yM-RQUi7R4VdiGTbO9EZh7b90cTkoOSc_GX48BpDIut7835VoUzj-Qin4BAmJ25_RnOqNZ8Dwqhseahu-muw4Oo-dAVJOP5Y6ACgrNh9y3SYXgbzAd_w355kKQYk_gM3GnjOSA' \
--header 'Content-Type: application/json' \
--data-raw '{
    "BatchId": "2c93525b-42d1-410a-9e26-aa957f19861d",
    "SourceSystem": "DEBTMAN",
    "TenantCode": "ZANBL",
    "ChannelID": null,
    "AudienceID": null,
    "Product": "DEBTMAN",
    "JobName": "DEBTMAN",
    "UniqueConsumerRef": "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
    "Timestamp": 1748351245.695410901,
    "RunPriority": null,
    "EventType": null,
    "BatchFiles": [
        {
            "ObjectId": "{1037A096-0000-CE1A-A484-3290CA7938C4}",
            "RepositoryId": "BATCH",
            "BlobUrl": "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
            "Filename": "DEBTMAN.csv",
            "FileType": "DATA",
            "ValidationStatus": "valid"
        }
    ]
}'
