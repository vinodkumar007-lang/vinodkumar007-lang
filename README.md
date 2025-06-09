import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

String decodedUrl = URLDecoder.decode(summaryFileUrl, StandardCharsets.UTF_8.name());
summaryPayload.setSummaryFileURL(decodedUrl);
