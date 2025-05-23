We've noticed that our Kafka consumer service is currently receiving a mix of both old and new message formats from the topic. Specifically:

Some messages are in the old format, which includes fields like batchFiles (all lowercase).

Others are in the new format, where the same field appears as BatchFiles (with capital letters), along with several additional or updated fields.

This inconsistency is causing issues with parsing and downstream processing, especially since field names are case-sensitive and some fields are missing or differently structured in older messages.

Could you please confirm:

If this mixed-message format is expected?

Whether the old messages are being resent intentionally, or if it's residual data?

If there's a plan to standardize all messages to the new structure going forward?

Your assistance in clarifying this will help us ensure reliable and consistent processing
