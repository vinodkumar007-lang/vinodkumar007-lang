case "PRINT" -> {
        if (printFiles != null && !printFiles.isEmpty()) {
            entry.setPrintStatus("SUCCESS");
        } else {
            entry.setPrintStatus("");
        }
    }
