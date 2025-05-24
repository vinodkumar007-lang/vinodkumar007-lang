private SummaryPayload mergeSummaryPayloads(List<SummaryPayload> payloads) {
    if (payloads == null || payloads.isEmpty()) {
        return new SummaryPayload();
    }

    SummaryPayload merged = new SummaryPayload();

    HeaderInfo mergedHeader = new HeaderInfo();
    PayloadInfo mergedPayload = new PayloadInfo();
    Map<String, CustomerSummary> mergedCustomers = new HashMap<>();
    Set<String> printFileSet = new LinkedHashSet<>(); // avoid duplicates

    for (SummaryPayload p : payloads) {
        HeaderInfo h = p.getHeader();
        if (h != null) {
            if (isNonEmpty(h.getBatchId())) mergedHeader.setBatchId(h.getBatchId());
            if (isNonEmpty(h.getBatchStatus())) mergedHeader.setBatchStatus(h.getBatchStatus());
            if (isNonEmpty(h.getJobName())) mergedHeader.setJobName(h.getJobName());
            if (isNonEmpty(h.getSourceSystem())) mergedHeader.setSourceSystem(h.getSourceSystem());
            if (isNonEmpty(h.getTenantCode())) mergedHeader.setTenantCode(h.getTenantCode());
            if (isNonEmpty(h.getChannelID())) mergedHeader.setChannelID(h.getChannelID());
            if (isNonEmpty(h.getAudienceID())) mergedHeader.setAudienceID(h.getAudienceID());
        }

        PayloadInfo pl = p.getPayload();
        if (pl != null) {
            if (isNonEmpty(pl.getUniqueConsumerRef())) mergedPayload.setUniqueConsumerRef(pl.getUniqueConsumerRef());
            if (isNonEmpty(pl.getUniqueECPBatchRef())) mergedPayload.setUniqueECPBatchRef(pl.getUniqueECPBatchRef());
            if (isNonEmpty(pl.getRunPriority())) mergedPayload.setRunPriority(pl.getRunPriority());
            if (isNonEmpty(pl.getEventID())) mergedPayload.setEventID(pl.getEventID());
            if (isNonEmpty(pl.getEventType())) mergedPayload.setEventType(pl.getEventType());
            if (isNonEmpty(pl.getRestartKey())) mergedPayload.setRestartKey(pl.getRestartKey());

            if (pl.getPrintFiles() != null) {
                printFileSet.addAll(pl.getPrintFiles());
            }
        }

        if (p.getMetaData() != null && p.getMetaData().getCustomerSummaries() != null) {
            for (CustomerSummary cs : p.getMetaData().getCustomerSummaries()) {
                if (cs == null || cs.getCustomerId() == null) continue;
                CustomerSummary existing = mergedCustomers.get(cs.getCustomerId());
                if (existing == null) {
                    mergedCustomers.put(cs.getCustomerId(), cs);
                } else {
                    if (cs.getFiles() != null) {
                        if (existing.getFiles() == null) {
                            existing.setFiles(new ArrayList<>());
                        }
                        existing.getFiles().addAll(cs.getFiles());
                    }
                }
            }
        }
    }

    mergedPayload.setPrintFiles(new ArrayList<>(printFileSet));
    merged.setHeader(mergedHeader);
    merged.setPayload(mergedPayload);

    MetaDataInfo metaDataInfo = new MetaDataInfo();
    metaDataInfo.setCustomerSummaries(new ArrayList<>(mergedCustomers.values()));
    merged.setMetaData(metaDataInfo);

    return merged;
}
private boolean isNonEmpty(String s) {
    return s != null && !s.trim().isEmpty();
}
