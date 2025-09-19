I hope you’re doing well.

We have completed Audit changes ,While doing testing trying to send audit messages from our Kubernetes pod in Azure to the log-ecp-batch-audit topic in DEV. While the main topic (str-ecp-batch-composition-complete) works fine, the audit messages are failing with a timeout.

After investigation, we found the following:

Seems to be The log-ecp-batch-audit topic is currently hosted on Onprem Kafka brokers:

nbpigelpdev02.africa.nedcor.net:9093
nbpproelpdev01.africa.nedcor.net:9093
nbpinelpdev01.africa.nedcor.net:9093

From our Kubernetes pod in Azure, these Onprem brokers are not reachable, resulting in timeouts:

[jboss@file-manager-655fbd4db5-np8dl app]$ curl -v telnet://nbpigelpdev02.africa.nedcor.net:9093
* Trying 10.58.150.57...
* TCP_NODELAY set
* connect to 10.58.150.57 port 9093 failed: Connection timed out
* Failed to connect to nbpigelpdev02.africa.nedcor.net port 9093: Connection timed out

Request:

To resolve this and allow our Kubernetes pod to send audit events reliably, we request the following:

Create the log-ecp-batch-audit topic on Azure Kafka, similar to str-ecp-batch-composition.

Provide the Azure bootstrap servers and ensure port 9093 is accessible from our pods.

Share the necessary SSL certificates/truststore details for secure connectivity.

Please confirm that the audit topic and corresponding Azure Kafka servers are correctly installed and accessible so we can proceed with testing.

Once this is done, our audit messages should flow correctly without timeout issues.

Please let us know if any further details or tests are required from our side.

Thank you for your support.
