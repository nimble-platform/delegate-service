import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.filter.ThresholdFilter;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.StringWriter;

import static org.apache.commons.io.IOUtils.*;

/**
 * Created by evgeniyh on 8/21/18.
 */

@ApplicationPath("/")
@Path("/")
public class Main {
    private static Logger logger = LogManager.getLogger(Main.class);

    static {
        logger.info("Retrieving all the registered delegates");
        try {
            String delegatesString = executeHttpGet("http://localhost:1000/eureka/delegate/apps", true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GET
    public Response getHello() {
        return Response.status(Response.Status.OK).entity("Hello from Delegate service").build();
    }

    public static String executeHttpGet(String url, boolean logResponse) throws Exception {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            httpGet.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpclient.execute(httpGet);
            if (response == null) {
                throw new RuntimeException("http response was null for - " + url);
            }
            String responseString = inputStreamToString(response.getEntity().getContent());
            if (logResponse) {
                logger.info(String.format("Response for url - %s was - %s", url, responseString));
            }
            return responseString;
        } catch (Throwable t) {
            logger.error("Error during execution of GET on - " + url, t);
            throw t;
        }
    }

    public static String inputStreamToString(InputStream stream) {
        try {
            StringWriter writer = new StringWriter();
            copy(stream, writer, "UTF-8");
            return writer.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
