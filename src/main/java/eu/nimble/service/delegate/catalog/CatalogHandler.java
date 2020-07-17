package eu.nimble.service.delegate.catalog;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.nimble.service.delegate.eureka.ServiceEndpoint;

/**
 * Catalog Service Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 08/05/2019.
 */
public class CatalogHandler {
	private static Logger logger = LogManager.getLogger(CatalogHandler.class);
	
    private static String SERVICE_URL = "CATALOG_SERVICE_BASE_URL";
    private static String SERVICE_PORT = "CATALOG_SERVICE_PORT";
	
    public static String GET_CATALOG_PATH= "/catalogue/%s/%s";
    public static String GET_CATALOG_LOCAL_PATH= "/catalogue/%s/%s/local";
    public static String GET_CATALOG_LINE_BY_HJID_PATH = "/catalogueline/%s";
    public static String GET_CATALOG_LINE_BY_HJID_LOCAL_PATH = "/catalogueline/%s/local";
	public static String GET_CATALOG_LINES_BY_HJIDS_PATH = "/cataloguelines";
	public static String GET_CATALOG_LINES_BY_HJIDS_LOCAL_PATH = "/cataloguelines/local";
    public static String GET_CATALOG_LINE_PATH = "/catalogue/%s/catalogueline";
    public static String GET_CATALOG_LINE_LOCAL_PATH = "/catalogue/%s/catalogueline/local";
    public static String GET_BINARY_CONTENT_PATH = "/binary-content";
    public static String GET_BINARY_CONTENT_LOCAL_PATH = "/binary-content/local";
    public static String GET_CATALOG_LINES_PATH = "/catalogue/%s/cataloguelines";
    public static String GET_CATALOG_LINES_LOCAL_PATH = "/catalogue/%s/cataloguelines/local";
    public static String GET_MULTIPLE_CATALOG_LINES_PATH = "/catalogue/cataloguelines";
    public static String GET_MULTIPLE_CATALOG_LINES_LOCAL_PATH = "/catalogue/cataloguelines/local";
	public static String GET_PRODUCT_STATUS_PATH = "/catalogue/cataloguelines/valid";
	public static String GET_PRODUCT_STATUS_LOCAL_PATH = "/catalogue/cataloguelines/valid/local";
    public static String GET_BINARY_CONTENTS_PATH = "/binary-contents";
    public static String GET_BINARY_CONTENTS_LOCAL_PATH = "/binary-contents/local";
    public static String GET_CONTRACT_FOR_CATALOGUE = "/catalogue/contract";
	public static String GET_CONTRACT_FOR_CATALOGUE_LOCAL_PATH = "/catalogue/contract/local";
    
    public String BaseUrl;
    public int Port;
    public String PathPrefix;
	
	public CatalogHandler() {
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
		
		logger.info("Service Handler is being initialized with base url = " + BaseUrl + ", path prefix = " + PathPrefix + ", port = " + Port + "...");
	}
	
	// in most catalog service calls, we can't know which catalog service is the right one to send the request to.
	// the solution - we send the request to all delegates, all except for one should return back 404 (not found) while only one 
	// should return back the real result.
	// this function is used to build the single response from all delegates responses.
	public Response buildResponseFromSingleDelegate(HashMap<ServiceEndpoint, String> responses) {
		if (responses.size() == 0) {
    		return Response.status(Response.Status.NOT_FOUND).build();
    	}
    	if (responses.size() > 1) {
    		logger.error("somehow got multiple responses...");
    		return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    	}
		for (ServiceEndpoint endpoint : responses.keySet()) {
    		String results = responses.get(endpoint);
    		return Response.status(Response.Status.OK)
    				.type(MediaType.APPLICATION_JSON)
    				.entity(results)
    				.build();
    	}
		return null;
	}

	public static String mergeListResults(HashMap<ServiceEndpoint, String> delegateResponses){
		JsonArray jsonArray = new JsonArray();

		JsonParser jsonParser = new JsonParser();

		for (String value : delegateResponses.values()) {
			JsonArray elements = (JsonArray) jsonParser.parse(value);
			for (JsonElement element : elements) {
				jsonArray.add(element);
			}
		}
		return jsonArray.toString();
	}

	public static String mergeMapResults(HashMap<ServiceEndpoint, String> delegateResponses){
		JsonObject jsonObject = new JsonObject();

		JsonParser jsonParser = new JsonParser();

		Set<String> keys = null;

		for (String value : delegateResponses.values()) {
			JsonObject elements = (JsonObject) jsonParser.parse(value);
			if(keys == null){
				keys = elements.keySet();
			}
			for (String key : keys) {
				if(elements.get(key).getAsJsonArray().size() != 0){
					jsonObject.add(key,elements.get(key));
				}
			}
		}

		for (String key : keys) {
			if(jsonObject.get(key) == null){
				jsonObject.add(key,new JsonArray());
			}
		}

		return jsonObject.toString();
	}
}
