import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import java.util.*;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessedFileEntry {
    private String customerId;
    private String accountNumber;

    private String pdfEmailFileUrl;
    private String pdfEmailFileUrlStatus;

    private String pdfMobstatFileUrl;
    private String pdfMobstatFileUrlStatus;

    private String printFileUrl;
    private String printFileUrlStatus;

    private String archiveBlobUrl;
    private String archiveStatus;

    private String overallStatus;

    public static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList,
                                                                      Map<String, ErrorReportEntry> errorMap) {
        Map<String, ProcessedFileEntry> groupedMap = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedList) {
            String key = file.getCustomerId() + "_" + file.getAccountNumber();
            groupedMap.putIfAbsent(key, new ProcessedFileEntry());
            ProcessedFileEntry entry = groupedMap.get(key);

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            switch (file.getOutputType()) {
                case "EMAIL":
                    entry.setPdfEmailFileUrl(file.getBlobUrl());
                    entry.setPdfEmailFileUrlStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    entry.setPdfMobstatFileUrl(file.getBlobUrl());
                    entry.setPdfMobstatFileUrlStatus(file.getStatus());
                    break;
                case "PRINT":
                    entry.setPrintFileUrl(file.getBlobUrl());
                    entry.setPrintFileUrlStatus(file.getStatus());
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(file.getBlobUrl());
                    entry.setArchiveStatus(file.getStatus());
                    break;
            }
        }

        for (ProcessedFileEntry entry : groupedMap.values()) {
            String email = entry.getPdfEmailFileUrlStatus();
            String mobstat = entry.getPdfMobstatFileUrlStatus();
            String print = entry.getPrintFileUrlStatus();
            String archive = entry.getArchiveStatus();

            boolean hasEmail = email != null;
            boolean hasMobstat = mobstat != null;
            boolean hasPrint = print != null;
            boolean hasArchive = archive != null && archive.equalsIgnoreCase("SUCCESS");

            List<String> allStatuses = new ArrayList<>();

            if (hasEmail) allStatuses.add(email);
            if (hasMobstat) allStatuses.add(mobstat);
            if (hasPrint) allStatuses.add(print);

            if (allStatuses.stream().allMatch(s -> s.equalsIgnoreCase("SUCCESS")) && hasArchive) {
                entry.setOverallStatus("SUCCESS");
            } else if (allStatuses.stream().anyMatch(s -> s.equalsIgnoreCase("FAILED")) || !hasArchive) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }
        }

        return new ArrayList<>(groupedMap.values());
    }
}

