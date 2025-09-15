# Orchestration runtime base URL (without jobId)
ot.runtime.url=http://exstream-deployment-orchestration-service.dev-exstream:8300/orchestration/api/v1/runtime/dev-SA/jobs/

@Value("${ot.runtime.url}")
private String runtimeBaseUrl;

String runtimeUrl = runtimeBaseUrl + otResponse.getJobId();
