However, I do not have edit access to the POD environment to check or modify the truststore configuration. By default, the JDK truststore is used (cacerts), but if the POD is configured to use a custom truststore, the certificate may also need to be imported there.

Could you please check if a custom truststore is being used on the POD? If so, I kindly request your help to import the OpenText certificate into that truststore to resolve the SSL handshake issue.
