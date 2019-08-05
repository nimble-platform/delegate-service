package eu.nimble.service.delegate.identity;

import java.net.URI;
import java.util.Arrays;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.nimble.service.delegate.http.HttpHelper;

/**
 * Identity Service Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 08/05/2019.
 */
public class IdentityHandler {
	private static Logger logger = LogManager.getLogger(IdentityHandler.class);
	
    private static String SERVICE_URL = "IDENTITY_SERVICE_BASE_URL";
    private static String SERVICE_PORT = "IDENTITY_SERVICE_PORT";
	
	public static String GET_USER_INFO_PATH = "/user-info";
	
    public static String BaseUrl;
    public static int Port;
    public static String PathPrefix;
	
	private HttpHelper _httpHelper;
	
	public IdentityHandler(HttpHelper httpHelper) {
		try {
			BaseUrl = System.getenv(SERVICE_URL);
			try {
				Port = Integer.parseInt(System.getenv(SERVICE_PORT));
			}
			catch (Exception ex) {
				Port = -1;
			}
			String[] serviceUrlParts = BaseUrl.split("/");
			if (serviceUrlParts.length > 1) {
				BaseUrl = serviceUrlParts[0];
				PathPrefix = "/"+String.join("/", Arrays.copyOfRange(serviceUrlParts, 1, serviceUrlParts.length));
			}
			else {
				PathPrefix = "";
			}
		}
		catch (Exception ex) {
    		logger.error("service env vars are not set as expected");
    	}
		
		this._httpHelper = httpHelper;
		
		logger.info("Service Handler is being initialized with base url = " + BaseUrl + ", path prefix = " + PathPrefix + ", port = " + Port + "...");
	}
	
	public boolean userExist(String accessToken) {
		URI uri = _httpHelper.buildUri(BaseUrl, Port, PathPrefix+GET_USER_INFO_PATH, null);
        logger.info("sending a request to " + uri.toString() + " in order to get user info, based on a give access token");
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Accept", "application/json");
        headers.add("Authorization", accessToken);
        
        Response response = _httpHelper.sendGetRequest(uri, headers);
        
        logger.info("got reseponse from identity service: " + response.readEntity(String.class));
        if (response.getStatus() >= 200 && response.getStatus() < 300) { // success
        	return true;
        }
        return false;
	}
}
