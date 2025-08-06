This is the error when trying to get that file
 
2025-08-05T14:41:09.221+02:00  INFO 8782236 --- [ecmbatch-print-listener] [http-nio-9013-exec-1] z.c.n.e.e.controller.TestController      : Request to download file: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/6f86231b-f6c7-4108-a213-fdf18046c53a/19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs/summary_6f86231b-f6c7-4108-a213-fdf18046c53a.json

2025-08-05T14:41:13.142+02:00 DEBUG 8782236 --- [ecmbatch-print-listener] [http-nio-9013-exec-1] o.s.web.servlet.DispatcherServlet        : Failed to complete request: com.azure.storage.blob.models.BlobStorageException: Status code 404, "?<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.

RequestId:6b436600-701e-0009-4806-06ed44000000

Time:2025-08-05T12:41:12.7976975Z</Message></Error>"

2025-08-05T14:41:13.144+02:00 ERROR 8782236 --- [ecmbatch-print-listener] [http-nio-9013-exec-1] o.a.c.c.C.[.[.[/].[dispatcherServlet]    : Servlet.service() for servlet [dispatcherServlet] in context with path [] threw exception [Request processing failed: com.azure.storage.blob.models.BlobStorageException: Status code 404, "?<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.

RequestId:6b436600-701e-0009-4806-06ed44000000

Time:2025-08-05T12:41:12.7976975Z</Message></Error>"] with root cause
 
com.azure.storage.blob.models.BlobStorageException: Status code 404, "?<?xml version="1.0" encoding="utf-8"?><Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.

RequestId:6b436600-701e-0009-4806-06ed44000000

Time:2025-08-05T12:41:12.7976975Z</Message></Error>"

	at java.base/java.lang.invoke.MethodHandle.invokeWithArguments(Unknown Source) ~[na:na]

 
