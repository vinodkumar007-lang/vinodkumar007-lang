import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

for (PrintFiles pf : printFilesList) {
    if (pf.getPrintFileUrl() != null) {
        String decodedUrl = URLDecoder.decode(pf.getPrintFileUrl(), StandardCharsets.UTF_8);
        pf.setPrintFileUrl(decodedUrl);
    }
}
