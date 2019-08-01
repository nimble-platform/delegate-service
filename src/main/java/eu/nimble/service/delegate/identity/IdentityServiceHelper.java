package eu.nimble.service.delegate.identity;

import java.net.URI;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.nimble.service.delegate.http.HttpHelper;

public class IdentityServiceHelper {
	private static Logger logger = LogManager.getLogger(IdentityServiceHelper.class);
	
	public static String getUserInfoPath = "/user-info";
	
	private HttpHelper httpHelper;
	
	public IdentityServiceHelper(HttpHelper httpHelper) {
		this.httpHelper = httpHelper;
	}
	
	public boolean userExist(String identityServiceBaseUrl, int identityServicePort, String identityServicRelativePath, String accessToken) {
		URI uri = httpHelper.buildUri(identityServiceBaseUrl, identityServicePort, identityServicRelativePath, null);
        logger.info("sending a request to " + uri.toString() + " in order to get user info, based on a give access token");
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Accept", "application/json");
        headers.add("Authorization", accessToken);
        
        Response response = httpHelper.sendGetRequest(uri, headers);
        
        logger.info("got reseponse from identity service: " + response.readEntity(String.class));
        if (response.getStatus() >= 200 && response.getStatus() < 300) { // success
        	return true;
        }
        return false;
	}
}
