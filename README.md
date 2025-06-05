C:\Users\CC437236>curl -X POST https://dev-exstream.nednet.co.za/file-manager/api/file/process
{"timestamp":"2025-06-05T14:35:56.324+00:00","status":404,"error":"Not Found","path":"/file-manager/api/file/process"}
C:\Users\CC437236>curl https://dev-exstream.nednet.co.za/file-manager/api/file/health
{"timestamp":"2025-06-05T14:36:25.017+00:00","status":404,"error":"Not Found","path":"/file-manager/api/file/health"}
C:\Users\CC437236>curl -X GET https://dev-exstream.nednet.co.za/file-manager/api/file/health
{"timestamp":"2025-06-05T14:37:10.001+00:00","status":404,"error":"Not Found","path":"/file-manager/api/file/health"}
C:\Users\CC437236>curl -X POST http://localhost:8080/file-manager/api/file/process
{"timestamp":"2025-06-05T14:38:22.088+00:00","status":404,"error":"Not Found","path":"/file-manager/api/file/process"}
C:\Users\CC437236>curl -i localhost:9091/api/file/health
curl: (7) Failed to connect to localhost port 9091 after 2253 ms: Could not connect to server

C:\Users\CC437236>curl -x POST localhost:9091/api/file/process
curl: (5) Could not resolve proxy: POST
