List<String> statuses = Arrays.asList(
    Optional.ofNullable(spf.getPdfEmailStatus()).orElse(""),
    Optional.ofNullable(spf.getPdfArchiveStatus()).orElse(""),
    Optional.ofNullable(spf.getHtmlEmailStatus()).orElse(""),
    Optional.ofNullable(spf.getPdfMobstatStatus()).orElse(""),
    Optional.ofNullable(spf.getTxtEmailStatus()).orElse("")
);
