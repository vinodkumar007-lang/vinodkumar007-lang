We have consolidated all file upload methods into a single unified uploadFile(Object input, String targetPath) method, which internally handles different input types (String, byte[], File, and Path).

This reduces code duplication and simplifies maintenance, while preserving support for all previous input forms. The method uses type-checking internally and delegates the actual upload to a single uploadToBlobStorage() method.

Manual MIME type checks have been removed. We now use Apache Tika, a robust content detection library, which automatically detects the correct MIME type based on file content rather than just file extensions.

This ensures more accurate and reliable content-type handling, and also avoids hardcoding or maintaining MIME type mappings manually.
