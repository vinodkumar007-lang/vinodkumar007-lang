curl --location --request POST 'https://dev-exstream.nednet.co.za/otds/otdstenant/dev-exstream/otdsws/login' \
--header 'Cache-Control: no-cache' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--header 'Cookie: XSRF-TOKEN=25119625-9a77-43b2-8793-6a0e6c537bab' \
--data-urlencode 'grant_type=password' \
--data-urlencode 'username=tenantadmin' \
--data-urlencode 'password=Exstream1!' \
--data-urlencode 'client_id=devexstreamclient' \
--data-urlencode 'client_secret=nV6A23zcFU5bK6lwm5KLBY2r5Sm3bh5l'
