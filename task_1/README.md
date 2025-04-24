# kafka-production


![Kafka](kafka1.png "Kafka")
![Kafka](kafka2.png "Kafka")


# Регистрируем схему для ключа
```
jq -n --slurpfile data schema-key.json \
'{
  "schemaType": "JSON",
  "schema": $data[0] | tojson
}' | curl -s \
  --request POST \
  --url 'https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443/subjects/test-topic-key/versions' \
  --user 'producer-consumer:1qazXSW@' \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --cacert /Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  --data "@-"


```
{"id":1}%                                                                                              


# Регистрируем схему для значения
```

jq -n --slurpfile data schema-value.json \
'{
  "schemaType": "JSON",
  "schema": $data[0] | tojson
}' | curl -s \
  --request POST \
  --url 'https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443/subjects/test-topic-value/versions' \
  --user 'producer-consumer:1qazXSW@' \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --cacert /Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  --data "@-"

{"id":2}% 
```

```

python producer.py
INFO:httpx:HTTP Request: POST https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net/subjects/test-topic-key/versions?normalize=False "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: POST https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net/subjects/test-topic-value/versions?normalize=False "HTTP/1.1 200 OK"
INFO:__main__:Message delivered to test-topic [1] @ 2
INFO:__main__:Message successfully produced
```

```

python consumer.py
INFO:httpx:HTTP Request: GET https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net/subjects/test-topic-key/versions/latest "HTTP/1.1 200 OK"
INFO:httpx:HTTP Request: GET https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net/subjects/test-topic-value/versions/latest "HTTP/1.1 200 OK"

Received message:
Key: {
  "id": "msg-123"
}
Value: {
  "text": "Тестовое сообщение",
  "timestamp": "2025-04-24T14:41:54.272482"
}
Partition: 1, Offset: 2
```

```

curl -v -X GET \
  -u 'producer-consumer:1qazXSW@' \
  --cacert /Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443/subjects
Note: Unnecessary use of -X or --request, GET is already inferred.
* Host rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443 was resolved.
* IPv6: (none)
* IPv4: 158.160.36.161
*   Trying 158.160.36.161:443...
* Connected to rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net (158.160.36.161) port 443
* ALPN: curl offers h2,http/1.1
* (304) (OUT), TLS handshake, Client hello (1):
*  CAfile: /Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
*  CApath: none
* (304) (IN), TLS handshake, Server hello (2):
* TLSv1.2 (IN), TLS handshake, Certificate (11):
* TLSv1.2 (IN), TLS handshake, Server key exchange (12):
* TLSv1.2 (IN), TLS handshake, Server finished (14):
* TLSv1.2 (OUT), TLS handshake, Client key exchange (16):
* TLSv1.2 (OUT), TLS change cipher, Change cipher spec (1):
* TLSv1.2 (OUT), TLS handshake, Finished (20):
* TLSv1.2 (IN), TLS change cipher, Change cipher spec (1):
* TLSv1.2 (IN), TLS handshake, Finished (20):
* SSL connection using TLSv1.2 / ECDHE-RSA-AES128-GCM-SHA256 / [blank] / UNDEF
* ALPN: server accepted h2
* Server certificate:
*  subject: CN=rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net
*  start date: Apr 24 08:51:57 2025 GMT
*  expire date: Apr 24 08:51:57 2026 GMT
*  subjectAltName: host "rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net" matched cert's "rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net"
*  issuer: DC=ru; DC=yandex; DC=ld; CN=YandexCLCA
*  SSL certificate verify ok.
* using HTTP/2
* Server auth using Basic with user 'producer-consumer'
* [HTTP/2] [1] OPENED stream for https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443/subjects
* [HTTP/2] [1] [:method: GET]
* [HTTP/2] [1] [:scheme: https]
* [HTTP/2] [1] [:authority: rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net]
* [HTTP/2] [1] [:path: /subjects]
* [HTTP/2] [1] [authorization: Basic cHJvZHVjZXItY29uc3VtZXI6MXFhelhTV0A=]
* [HTTP/2] [1] [user-agent: curl/8.7.1]
* [HTTP/2] [1] [accept: */*]
> GET /subjects HTTP/2
> Host: rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net
> Authorization: Basic cHJvZHVjZXItY29uc3VtZXI6MXFhelhTV0A=
> User-Agent: curl/8.7.1
> Accept: */*
> 
* Request completely sent off
< HTTP/2 200 
< server: nginx
< date: Thu, 24 Apr 2025 11:43:56 GMT
< content-type: application/vnd.schemaregistry.v1+json
< content-length: 37
< access-control-allow-origin: *
< access-control-allow-methods: DELETE, GET, OPTIONS, POST, PUT
< access-control-allow-headers: Authorization, Content-Type
< access-control-expose-headers: etag
< etag: "4b1cdcb0ef15ccdcb486db57778d0e10"
< strict-transport-security: max-age=31536000
< x-frame-options: SAMEORIGIN
< x-content-type-options: nosniff
< x-xss-protection: 1; mode=block
< 
* Connection #0 to host rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net left intact
["test-topic-key","test-topic-value"]% 
```

```

curl -X GET \
  -u 'producer-consumer:1qazXSW@' \
  --cacert /Users/ivan/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
  'https://rc1a-7jfoft6ot385o5ui.mdb.yandexcloud.net:443/subjects/test-topic-value/versions'
[1]%   
```

```

python get_topics.py

Topic: test-topic
PartitionCount: 3
ReplicationFactor: 3
        Partition: 0
        Leader: 3
        Replicas: [3, 1, 2]
--------------------------------------------------
        Partition: 1
        Leader: 1
        Replicas: [1, 2, 3]
--------------------------------------------------
        Partition: 2
        Leader: 2
        Replicas: [2, 3, 1]
--------------------------------------------------
```
