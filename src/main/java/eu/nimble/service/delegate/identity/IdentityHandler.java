package eu.nimble.service.delegate.identity;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.nimble.service.delegate.http.HttpHelper;

/**
 * Identity Service Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 08/05/2019.
 */
public class IdentityHandler {
	private static Logger logger = LogManager.getLogger(IdentityHandler.class);
	
    public static String POST_LOGIN_PATH = "/login";
	public static String GET_USER_INFO_PATH = "/user-info";
	
    private String _baseUrl;
    private int _port;
    private String _pathPrefix;
    private String _username;
    private String _password;
	
	private HttpHelper _httpHelper;
	private static ObjectMapper _mapper;
	
	public IdentityHandler(HttpHelper httpHelper, String baseUrl, int port, String pathPrefix, String username, String password) {
		_baseUrl = baseUrl;
		_port = port;
		_pathPrefix = pathPrefix;
		_username = username;
		_password = password;
		
		this._httpHelper = httpHelper;
		
		_mapper = new ObjectMapper();
		
		logger.info("Identity Service Handler is being initialized with base url = " + _baseUrl + ", path prefix = " + _pathPrefix + ", port = " + _port + "...");
	}
	
	@SuppressWarnings("unchecked")
	public String getAccessToken() throws JsonParseException, JsonMappingException, IOException {
		Map<String, Object> body = new HashMap<String, Object>();
		body.put("username", _username);
		body.put("password", _password);
		
		URI uri = _httpHelper.buildUri(_baseUrl, _port, _pathPrefix+POST_LOGIN_PATH, null);
        logger.info("sending a request to " + uri.toString() + " in order to login using username and password");
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Accept", "application/json");
        headers.add("Content-Type", "application/json");
        
        Response response = _httpHelper.sendPostRequest(uri, headers, body);
        String userData = response.readEntity(String.class);
		Map<String, Object> json = _mapper.readValue(userData, Map.class);
        return json.get("accessToken").toString();
	}
	
	public boolean userExist(String accessToken) {
		if (accessToken == null) {
			return false;
		}
		// TODO remove this line of checking cross delegates requests
		if (accessToken.equals("delegate access token in the federation identity service")) {
			return true;
		}
		
		URI uri = _httpHelper.buildUri(_baseUrl, _port, _pathPrefix+GET_USER_INFO_PATH, null);
        logger.info("sending a request to " + uri.toString() + " in order to get user info, based on a given access token");
        
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
