Properties props = new Properties();

// Kafka broker addresses
props.put("bootstrap.servers", "nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093");

// Consumer group configuration
props.put("group.id", "str-ecp-batch");
props.put("enable.auto.commit", "false");
props.put("auto.offset.reset", "earliest");

// Deserialization
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

// SSL configuration
props.put("security.protocol", "SSL");
props.put("ssl.truststore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks");
props.put("ssl.truststore.password", "nedbank1");
props.put("ssl.keystore.location", "C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks");
props.put("ssl.keystore.password", "3dX7y3Yz9Jv6L4F");
props.put("ssl.key.password", "3dX7y3Yz9Jv6L4F");

// Optional: disable hostname verification if you're using self-signed certs or IPs
props.put("ssl.endpoint.identification.algorithm", "");

// Optional: enforce specific protocol version if needed
props.put("ssl.protocol", "TLSv1.2");
