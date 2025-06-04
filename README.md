2025-06-04 12:05:11.949221+00:00 [info] <0.573.0> Starting message stores for vhost '/'
2025-06-04 12:05:12.065685+00:00 [info] <0.583.0> Message store "628WB79CIFDYO9LJI6DKMI09L/msg_store_transient": using rabbit_msg_store_ets_index to provide index
2025-06-04 12:05:12.104135+00:00 [info] <0.573.0> Started message store of type transient for vhost '/'
2025-06-04 12:05:12.104372+00:00 [info] <0.587.0> Message store "628WB79CIFDYO9LJI6DKMI09L/msg_store_persistent": using rabbit_msg_store_ets_index to provide index
2025-06-04 12:05:12.143234+00:00 [info] <0.573.0> Started message store of type persistent for vhost '/'
2025-06-04 12:05:12.143458+00:00 [info] <0.573.0> Recovering 0 queues of type rabbit_classic_queue took 302ms
2025-06-04 12:05:12.656877+00:00 [notice] <0.600.0> queue 'exstream-orchestration-direct-msgs-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:12.724206+00:00 [info] <0.573.0> Recovering 9 queues of type rabbit_quorum_queue took 580ms
2025-06-04 12:05:12.724333+00:00 [info] <0.573.0> Recovering 0 queues of type rabbit_stream_queue took 0ms
2025-06-04 12:05:12.743657+00:00 [notice] <0.604.0> queue 'exstream-orchestration-main-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:12.811975+00:00 [notice] <0.602.0> queue 'exstream-orchestration-externalevent-input-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:12.821895+00:00 [notice] <0.598.0> queue 'exstream-import-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:12.880097+00:00 [info] <0.254.0> Running boot step empty_db_check defined by app rabbit
2025-06-04 12:05:12.880192+00:00 [info] <0.254.0> Will not seed default virtual host and user: have definitions to load...
2025-06-04 12:05:12.880283+00:00 [info] <0.254.0> Running boot step rabbit_observer_cli defined by app rabbit
2025-06-04 12:05:12.881100+00:00 [info] <0.254.0> Running boot step rabbit_looking_glass defined by app rabbit
2025-06-04 12:05:12.881169+00:00 [info] <0.254.0> Running boot step rabbit_core_metrics_gc defined by app rabbit
2025-06-04 12:05:12.883576+00:00 [info] <0.254.0> Running boot step background_gc defined by app rabbit
2025-06-04 12:05:12.883792+00:00 [info] <0.254.0> Running boot step routing_ready defined by app rabbit
2025-06-04 12:05:12.883831+00:00 [info] <0.254.0> Running boot step pre_flight defined by app rabbit
2025-06-04 12:05:12.883875+00:00 [info] <0.254.0> Running boot step notify_cluster defined by app rabbit
2025-06-04 12:05:12.883913+00:00 [info] <0.254.0> Running boot step networking defined by app rabbit
2025-06-04 12:05:12.883944+00:00 [info] <0.254.0> Running boot step rabbit_quorum_queue_periodic_membership_reconciliation defined by app rabbit
2025-06-04 12:05:12.884057+00:00 [info] <0.254.0> Running boot step definition_import_worker_pool defined by app rabbit
2025-06-04 12:05:12.884102+00:00 [info] <0.351.0> Starting worker pool 'definition_import_pool' with 4 processes in it
2025-06-04 12:05:12.884371+00:00 [info] <0.254.0> Running boot step cluster_name defined by app rabbit
2025-06-04 12:05:12.884467+00:00 [info] <0.254.0> Running boot step virtual_host_reconciliation defined by app rabbit
2025-06-04 12:05:12.922517+00:00 [info] <0.254.0> Running boot step direct_client defined by app rabbit
2025-06-04 12:05:12.922753+00:00 [info] <0.254.0> Running boot step rabbit_management_load_definitions defined by app rabbitmq_management
2025-06-04 12:05:12.922848+00:00 [info] <0.669.0> Resetting node maintenance status
2025-06-04 12:05:12.968112+00:00 [notice] <0.596.0> queue 'exstream-orchestration-externalevent-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:13.043959+00:00 [notice] <0.612.0> queue 'exstream-sps-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:13.120255+00:00 [notice] <0.608.0> queue 'exstream-orchestration-event-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:13.120347+00:00 [notice] <0.606.0> queue 'exstream-docgen-batch-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:13.238793+00:00 [notice] <0.610.0> queue 'exstream-orchestration-ei-qq' in vhost '/': candidate -> leader in term: 2 machine version: 3
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0> Deprecated features: `management_metrics_collection`: Feature `management_metrics_collection` is deprecated.
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0> By default, this feature can still be used for now.
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0> Its use will not be permitted by default in a future minor RabbitMQ version and the feature will be removed from a future major RabbitMQ version; actual versions to be determined.
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0> To continue using this feature when it is not permitted by default, set the following parameter in your configuration:
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0>     "deprecated_features.permit.management_metrics_collection = true"
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0> To test RabbitMQ as if the feature was removed, set this in your configuration:
2025-06-04 12:05:15.740043+00:00 [warning] <0.707.0>     "deprecated_features.permit.management_metrics_collection = false"
06/04/25 12:05:20: Checking to see if RabbitMQ is available...
curl: (7) Failed to connect to experience-deployment-rabbitmq-0.experience-deployment-rabbitmq-headless.dev-exstream.svc.cluster.local port 15672 after 0 ms: Could not connect to server
06/04/25 12:05:20: Waiting for Rabbitmq node to start.
06/04/25 12:05:30: Checking to see if RabbitMQ is available...
curl: (7) Failed to connect to experience-deployment-rabbitmq-0.experience-deployment-rabbitmq-headless.dev-exstream.svc.cluster.local port 15672 after 0 ms: Could not connect to server
06/04/25 12:05:30: Waiting for Rabbitmq node to start.
06/04/25 12:05:40: Checking to see if RabbitMQ is available...
curl: (7) Failed to connect to experience-deployment-rabbitmq-0.experience-deployment-rabbitmq-headless.dev-exstream.svc.cluster.local port 15672 after 0 ms: Could not connect to server
06/04/25 12:05:40: Waiting for Rabbitmq node to start.
06/04/25 12:05:50: Checking to see if RabbitMQ is available...
curl: (7) Failed to connect to experience-deployment-rabbitmq-0.experience-deployment-rabbitmq-headless.dev-exstream.svc.cluster.local port 15672 after 0 ms: Could not connect to server
06/04/25 12:05:51: Waiting for Rabbitmq node to start.
06/04/25 12:06:01: Checking to see if RabbitMQ is available...
curl: (7) Failed to connect to experience-deployment-rabbitmq-0.experience-deployment-rabbitmq-headless.dev-exstream.svc.cluster.local port 15672 after 0 ms: Could not connect to server
06/04/25 12:06:01: Waiting for Rabbitmq node to start.
2025-06-04 12:06:08.065789+00:00 [info] <0.744.0> Management plugin: HTTP (non-TLS) listener started on port 15672
 
