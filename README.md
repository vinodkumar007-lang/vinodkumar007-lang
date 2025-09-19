kafka-console-producer.sh \
  --broker-list nbpigelpdev02.africa.nedcor.net:9093 \
  --topic log-ecp-batch-audit \
  --producer.config /deployments/client.properties
