
I’ve removed the dedicated getOtdsToken() and now rely on the existing getSecret() method to fetch the OTDS token the same way as other Key Vault secrets. This keeps everything consistent and avoids duplicating logic.

Agreed – I’ve updated the code so deliveryFileMaps is initialized dynamically based on the deliveryFolders parameter instead of hardcoding folder names. This makes it easy to add new delivery types with a single config change, no code changes required.
