int totalFileUrls = processedFileEntries.stream()
        .mapToInt(entry -> {
            int count = 0;
            if (entry.getEmailFileURL() != null && !entry.getEmailFileURL().isBlank()) count++;
            if (entry.getArchiveFileURL() != null && !entry.getArchiveFileURL().isBlank()) count++;
            if (entry.getMobstatFileURL() != null && !entry.getMobstatFileURL().isBlank()) count++;
            if (entry.getPrintFileURL() != null && !entry.getPrintFileURL().isBlank()) count++;
            return count;
        })
        .sum();

payloadInfo.setFileCount(totalFileUrls);
