2025-05-09T15:03:02.131+02:00  INFO 16436 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
    "header": {
        "tenantCode": "ZANBL",
        "channelId": "123",
        "audienceId": "123",
        "timestamp": "2025-04-10 15:54:00:00",
        "source": "DEBTMAN",
        "product": "DEBTMAN",
        "jobName": "DebtManager"
    },
    "payload": {
        "consumerGuid": "sdasswwe-7d0b-421b-84ca-4492d6a22688",
        "ecpBatchGuid": "4ff35afc-7d0b-421b-84ca-4492d6a22688",
        "blobInputId": "1234567890",
        "runPriotrity": "01",
        "eventId": "1234567890"
    }
}
2025-05-09T15:03:02.186+02:00 ERROR 16436 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Error processing Kafka message: Failed to extract batchId from message: {
    "header": {
        "tenantCode": "ZANBL",
        "channelId": "123",
        "audienceId": "123",
        "timestamp": "2025-04-10 15:54:00:00",
        "source": "DEBTMAN",
        "product": "DEBTMAN",
        "jobName": "DebtManager"
    },
    "payload": {
        "consumerGuid": "sdasswwe-7d0b-421b-84ca-4492d6a22688",
        "ecpBatchGuid": "4ff35afc-7d0b-421b-84ca-4492d6a22688",
        "blobInputId": "1234567890",
        "runPriotrity": "01",
        "eventId": "1234567890"
    }
}

java.lang.RuntimeException: Failed to extract batchId from message: {
    "header": {
        "tenantCode": "ZANBL",
        "channelId": "123",
        "audienceId": "123",
        "timestamp": "2025-04-10 15:54:00:00",
        "source": "DEBTMAN",
        "product": "DEBTMAN",
        "jobName": "DebtManager"
    },
    "payload": {
        "consumerGuid": "sdasswwe-7d0b-421b-84ca-4492d6a22688",
        "ecpBatchGuid": "4ff35afc-7d0b-421b-84ca-4492d6a22688",
        "blobInputId": "1234567890",
        "runPriotrity": "01",
        "eventId": "1234567890"
    }
}
