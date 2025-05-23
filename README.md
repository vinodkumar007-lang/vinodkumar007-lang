package com.nedbank.kafka.filemanage.model;

import java.util.List;
import java.util.Map;

public class PayloadInfo {

    private List<Map<String, Object>> processedFiles;
    private List<Map<String, Object>> printFiles;

    public List<Map<String, Object>> getProcessedFiles() {
        return processedFiles;
    }

    public void setProcessedFiles(List<Map<String, Object>> processedFiles) {
        this.processedFiles = processedFiles;
    }

    public List<Map<String, Object>> getPrintFiles() {
        return printFiles;
    }

    public void setPrintFiles(List<Map<String, Object>> printFiles) {
        this.printFiles = printFiles;
    }
}
