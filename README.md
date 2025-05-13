curl --location --request POST 'https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/auth/userpass/login/espire_dev' \
--header 'x-vault-namespace: admin/espire' \
--header 'Content-Type: application/json' \
--data-raw '{
  "password": "Dev+Cred4#"
 }'
