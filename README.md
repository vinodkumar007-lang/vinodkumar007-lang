Enable SSL certificate verification
Verify SSL certificates when sending a request. Verification failures will result in the request being aborted.
Automatically follow redirects
Follow HTTP 3xx responses as redirects.
Follow original HTTP Method
Redirect with the original HTTP method instead of the default behavior of redirecting with GET.
Follow Authorization header
Retain authorization header when a redirect happens to a different hostname.
Remove referer header on redirect
Remove the referer header when a redirect happens.
Encode URL automatically
Encode the URL's path, query parameters, and authentication fields.
Disable cookie jar
Prevent cookies used in this request from being stored in the cookie jar. Existing cookies in the cookie jar will not be added as headers for this request.
Use server cipher suite during handshake
Use the server's cipher suite order instead of the client's during handshake.
Maximum number of redirects
Set a cap on the maximum number of redirects to follow.
10
Protocols disabled during handshake
Specify the SSL and TLS protocol versions to be disabled during handshake. All other protocols will be enabled.
Cipher suite selection
Order of cipher suites that the SSL server profile uses to establish a secure connection.
Enter cipher suites
