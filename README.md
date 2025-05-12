2025-05-12T10:29:43.518+02:00  INFO 20048 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.3.1
2025-05-12T10:29:43.521+02:00  INFO 20048 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: e23c59d00e687ff5
2025-05-12T10:29:43.521+02:00  INFO 20048 --- [           main] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1747038583516
2025-05-12T10:29:43.525+02:00  INFO 20048 --- [           main] o.a.k.clients.consumer.KafkaConsumer     : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Subscribed to topic(s): str-ecp-batch-composition
2025-05-12T10:29:43.549+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:29:43.562+02:00  INFO 20048 --- [           main] c.nedbank.kafka.filemanage.Application   : Started Application in 5.653 seconds (process running for 6.389)
2025-05-12T10:29:44.322+02:00  INFO 20048 --- [ntainer#0-0-C-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Resetting the last seen epoch of partition str-ecp-batch-composition-0 to 16 since the associated topicId changed from null to MwBBZLPpRK6MmJMBo7pw8g
2025-05-12T10:29:44.324+02:00  INFO 20048 --- [ntainer#0-0-C-1] org.apache.kafka.clients.Metadata        : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Cluster ID: y0ml4PnGSeO_hhGMyIz-pA
2025-05-12T10:29:44.327+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Discovered group coordinator nsnxeteelpka01.nednet.co.za:9093 (id: 2147483647 rack: null)
2025-05-12T10:29:44.330+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] (Re-)joining group
2025-05-12T10:29:44.425+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: need to re-join with the given member-id: consumer-str-ecp-batch-1-88775aae-27ce-4ee0-95ef-927e1f4dc87a
2025-05-12T10:29:44.425+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Request joining group due to: rebalance failed due to 'The group member needs to have a valid member id before actually entering a consumer group.' (MemberIdRequiredException)
2025-05-12T10:29:44.426+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] (Re-)joining group
2025-05-12T10:29:44.433+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Successfully joined group with generation Generation{generationId=31, memberId='consumer-str-ecp-batch-1-88775aae-27ce-4ee0-95ef-927e1f4dc87a', protocol='range'}
2025-05-12T10:29:44.435+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Finished assignment for group at generation 31: {consumer-str-ecp-batch-1-88775aae-27ce-4ee0-95ef-927e1f4dc87a=Assignment(partitions=[str-ecp-batch-composition-0])}
2025-05-12T10:29:44.445+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Successfully synced group in generation Generation{generationId=31, memberId='consumer-str-ecp-batch-1-88775aae-27ce-4ee0-95ef-927e1f4dc87a', protocol='range'}
2025-05-12T10:29:44.446+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Notifying assignor about the new Assignment(partitions=[str-ecp-batch-composition-0])
2025-05-12T10:29:44.449+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Adding newly assigned partitions: str-ecp-batch-composition-0
2025-05-12T10:29:44.466+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator  : [Consumer clientId=consumer-str-ecp-batch-1, groupId=str-ecp-batch] Setting offset for partition str-ecp-batch-composition-0 to the committed offset FetchPosition{offset=21, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[nsnxeteelpka03.nednet.co.za:9093 (id: 2 rack: null)], epoch=16}}
2025-05-12T10:29:44.468+02:00  INFO 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : str-ecp-batch: partitions assigned: [str-ecp-batch-composition-0]
2025-05-12T10:29:48.567+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:29:48.568+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:29:53.582+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:29:53.582+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:29:58.598+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:29:58.598+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:03.611+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:03.611+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:08.613+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:08.613+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:13.627+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:13.627+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:18.634+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:18.634+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:23.649+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:23.649+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:28.654+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:28.654+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:33.658+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:33.658+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:38.669+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:38.669+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:43.680+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:43.680+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:48.692+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:48.692+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:53.694+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:53.694+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:30:58.704+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:30:58.704+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:03.713+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:03.713+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:08.720+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:08.720+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:13.733+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:13.734+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:18.743+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:18.743+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:23.753+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:23.753+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:28.763+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:28.763+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:33.778+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:33.778+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:38.792+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:38.793+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:43.801+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:43.802+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:48.810+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:48.810+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:53.821+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:53.821+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:31:58.827+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:31:58.827+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:03.834+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:03.834+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:08.846+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:08.846+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:13.858+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:13.858+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:18.871+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:18.871+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:23.871+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:23.871+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:28.885+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:28.885+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:33.889+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:33.889+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:38.897+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:38.897+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:43.909+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:43.909+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:48.911+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:48.911+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:53.925+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:53.927+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:32:58.928+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:32:58.928+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:03.937+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:03.937+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:08.952+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:08.952+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:13.968+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:13.968+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:18.970+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:18.970+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:23.972+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:23.972+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:28.976+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:28.976+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:33.977+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:33.977+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:38.985+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:38.985+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:43.997+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:43.997+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:49.000+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:49.000+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:54.002+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:54.003+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:33:59.018+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:33:59.018+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:04.018+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:04.018+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:09.021+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:09.021+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:14.027+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:14.027+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:19.036+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:19.036+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:24.037+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:24.037+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:29.045+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:29.045+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:34.046+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:34.046+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:39.059+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:39.059+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:44.074+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:44.074+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:49.079+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:49.079+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:54.085+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:54.085+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:34:59.093+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:34:59.093+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:04.105+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:35:04.105+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:09.112+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:35:09.112+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:14.118+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:35:14.119+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:19.131+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:35:19.131+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:24.136+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-05-12T10:35:24.136+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-05-12T10:35:29.144+02:00 DEBUG 20048 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
