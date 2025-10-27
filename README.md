if (configOpt.isPresent()) {
    SourceSystemProperties.SystemConfig config = configOpt.get();
    url = config.getUrl();
    String secretName = config.getToken();  // token here means secret name
    token = blobStorageService.getSecret(secretName); // fetch actual token
    logger.info("Using URL={} for {}:{} with secretName={} fetched token successfully",
            url, message.getSourceSystem(), message.getJobName(), secretName);
} else {
    token = "";
    url = null;
    logger.warn("No config found for sourceSystem={} and jobName={}",
            message.getSourceSystem(), message.getJobName());
}
