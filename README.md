private static String determineOverallStatus(ProcessedFileEntry entry) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        int successCount = 0;
        int failedCount = 0;
        int notFoundCount = 0;

        List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
        for (String status : allStatuses) {
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else if ("NOT-FOUND".equalsIgnoreCase(status) || status == null || status.trim().isEmpty()) {
                notFoundCount++;
            }
        }

        // ✅ All 4 methods successful
        if (successCount == 4) {
            return "SUCCESS";
        }

        // ❌ At least one failed, none success
        if (failedCount > 0 && successCount == 0) {
            return "FAILED";
        }

        // ⚠️ At least one success, and at least one failed or not-found
        if (successCount > 0 && (failedCount > 0 || notFoundCount > 0)) {
            return "PARTIAL";
        }

        // ❌ All methods are NOT-FOUND (no outputs generated)
        if (notFoundCount == 4) {
            return "FAILED";
        }

        // Fallback safety
        return "PARTIAL";
    }
