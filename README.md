kafka:
  bootstrap:
    servers: nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093
  consumer:
    group:
      id: str-ecp-batch
    auto:
      offset:
        reset: earliest
    enable:
      auto:
        commit: true
    security:
      protocol: SSL
    ssl:
      keystore:
        location: C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
        password: 3dX7y3Yz9Jv6L4F
      key:
        password: 3dX7y3Yz9Jv6L4F
      truststore:
        location: C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
        password: nedbank1
      protocol: TLSv1.2
    key:
      deserializer: org.apache.kafka.common.serialization.StringDeserializer
    value:
      deserializer: org.apache.kafka.common.serialization.StringDeserializer
  producer:
    key:
      serializer: org.apache.kafka.common.serialization.StringSerializer
    value:
      serializer: org.apache.kafka.common.serialization.StringSerializer
    security:
      protocol: SSL
    ssl:
      keystore:
        location: C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\keystore.jks
        password: 3dX7y3Yz9Jv6L4F
      key:
        password: 3dX7y3Yz9Jv6L4F
      truststore:
        location: C:\\Users\\CC437236\\jdk-17.0.12_windows-x64_bin\\jdk-17.0.12\\lib\\security\\truststore.jks
        password: nedbank1
      protocol: TLSv1.2
    bootstrap:
      servers: nsnxeteelpka01.nednet.co.za:9093,nsnxeteelpka02.nednet.co.za:9093,nsnxeteelpka03.nednet.co.za:9093
  topic:
    input: str-ecp-batch-composition
    output: str-ecp-batch-composition-complete

azure:
  keyvault:
    uri: https://nsn-dev-ecm-kva-001.vault.azure.net/secrets
  blob:
    storage:
      account: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001

vault:
  hashicorp:
    url: https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200
    namespace: admin/espire
    passwordDev: Dev+Cred4#
    passwordNbhDev: nbh_dev1

logging:
  level:
    org:
      springframework:
        kafka: DEBUG
