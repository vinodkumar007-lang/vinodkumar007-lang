Request: curl --location --request GET 'https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/Store_Dev/10099' \
--header 'x-vault-namespace: admin/espire' \
--header 'x-vault-token: ' \
--header 'Content-Type: application/json' \
--data-raw '{
  "password": "nbh_dev1"
 }'

 response:
 {
    "request_id": "6d942d91-1e70-1cb7-3b8f-523745295748",
    "lease_id": "",
    "renewable": false,
    "lease_duration": 3600,
    "data": {
        "account_key": "",
        "account_name": "nsndvextr01",
        "container_name": "nsnakscontregecm001",
        "test": "test"
    },
    "wrap_info": null,
    "warnings": null,
    "auth": null,
    "mount_type": "kv"
}

Request authentication
curl --location --request POST 'https://vault-public-vault-75e984b5.bdecd756.z1.hashicorp.cloud:8200/v1/auth/userpass/login/espire_dev' \
--header 'x-vault-namespace: admin/espire' \
--header 'Content-Type: application/json' \
--data-raw '{
  "password": "Dev+Cred4#"
 }'

 Response authentication
 {
    "request_id": "fa09536a-a2fc-025f-950f-2afc9030e2fd",
    "lease_id": "",
    "renewable": false,
    "lease_duration": 0,
    "data": null,
    "wrap_info": null,
    "warnings": null,
    "auth": {
        "client_token": "",
        "accessor": "LUVKK2UO9KBfznC3BsVaAATl.l3FZm",
        "policies": [
            "default",
            "espire_dev"
        ],
        "token_policies": [
            "default",
            "espire_dev"
        ],
        "metadata": {
            "username": "espire_dev"
        },
        "lease_duration": 1200,
        "renewable": true,
        "entity_id": "b1fbda3d-3e91-ca3a-dd4d-0bc5812b6e28",
        "token_type": "service",
        "orphan": true,
        "mfa_requirement": null,
        "num_uses": 0
    },
    "mount_type": ""
}
