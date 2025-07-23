   private static String determineOverallStatus(ProcessedFileEntry entry) {
        String email = safeStatus(entry.getEmailStatus());
        String print = safeStatus(entry.getPrintStatus());
        String mobstat = safeStatus(entry.getMobstatStatus());
        String archive = safeStatus(entry.getArchiveStatus());

        int successCount = 0;
        int failedCount = 0;
        int notFoundCount = 0;

        List<String> allStatuses = Arrays.asList(email, print, mobstat, archive);
        for (String status : allStatuses) {
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else {
                // Covers "" or null or unknown statuses
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

        // ❌ All methods are not found or empty
        if (notFoundCount == 4) {
            return "FAILED";
        }

        // Fallback
        return "PARTIAL";
    }

    private static String safeStatus(String status) {
        return status != null ? status.trim().toUpperCase() : "";
    }
