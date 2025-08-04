Regarding the log-ecp-batch-audit Kafka topic you shared — could you please clarify the following:

Is our application expected to only consume from this topic to track audit logs from downstream systems?

Or are we also expected to publish audit events from our own processing steps (like input received, processing started, output sent) into this topic?

If publishing is expected, could you kindly share the expected JSON schema or sample message format for audit messages?

This will help us integrate it correctly in our File Manager flow.
