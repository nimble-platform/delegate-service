package eu.nimble.service.delegate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import eu.nimble.service.delegate.eureka.EurekaHandler;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;
import eu.nimble.service.delegate.http.HttpHelper;
import eu.nimble.service.delegate.identity.IdentityServiceHelper;
import eu.nimble.service.delegate.indexing.IndexingServiceHelper;
import eu.nimble.service.delegate.indexing.IndexingServiceResult;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.net.URI;

/**
 * Delegate service.
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 05/30/2019.
 */
@ApplicationPath("/")
@Path("/")
public class Delegate implements ServletContextListener {
    private static Logger logger = LogManager.getLogger(Delegate.class);

    private static EurekaHandler eurekaHandler;
    private static HttpHelper httpHelper;
    private static IndexingServiceHelper indexingServiceHelper;
    private static IdentityServiceHelper identityServiceHelper;
    
    //frontend service
    private static String frontendServiceUrl;
    // indexing service
    private static String indexingServiceBaseUrl;
    private static int indexingServicePort;
    private static String indexingServicePathPrefix;
    // identity service
    
    // catalog service
    
    /***********************************   Servlet Context   ***********************************/
    
    public void contextInitialized(ServletContextEvent arg0) 
    {
    	try {
    		frontendServiceUrl = System.getenv("FRONTEND_URL");
    		indexingServiceBaseUrl = System.getenv("INDEXING_SERVICE_URL");
    		try {
    			indexingServicePort = Integer.parseInt(System.getenv("INDEXING_SERVICE_PORT"));
    		}
    		catch (Exception ex) {
    			indexingServicePort = -1;
    		}
    		String[] indexingServiceUrlParts = indexingServiceBaseUrl.split("/");
    		if (indexingServiceUrlParts.length > 1) {
    			indexingServiceBaseUrl = indexingServiceUrlParts[0];
    			indexingServicePathPrefix = "/"+String.join("/", Arrays.copyOfRange(indexingServiceUrlParts, 1, indexingServiceUrlParts.length));
    		}
    		else {
    			indexingServicePathPrefix = "";
    		}
    	}
    	catch (Exception ex) {
    		logger.warn("env vars are not set as expected");
    	}
    	
        logger.info("Delegate service is being initialized with frontend service param = " + frontendServiceUrl 
        											+ ", indexing service base url = " + indexingServiceBaseUrl 
        											+ ", indexing service prefix = " + indexingServicePathPrefix 
        											+ ", indexing service port = " + indexingServicePort + "...");
        
        eurekaHandler = new EurekaHandler();
        if (!eurekaHandler.initEureka()) {
            logger.error("Failed to initialize Eureka client");
            return;
        }
        
        httpHelper = new HttpHelper(eurekaHandler);
        indexingServiceHelper = new IndexingServiceHelper(httpHelper, eurekaHandler);
        identityServiceHelper = new IdentityServiceHelper(httpHelper);
        
        logger.info("Delegate service has been initialized");
    }

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
    	eurekaHandler.destroy();
        logger.info("Delegate service has been destroyed");
    }
    
    /***********************************   Servlet Context - END   ***********************************/
    
    @GET
    @Path("/")
    public Response hello() {
        return Response.status(Status.OK)
        			   .type(MediaType.TEXT_PLAIN)
        			   .entity("Hello from Delegate Service\n")
        			   .build();
    }
    
    @GET
    @Path("eureka")
    @Produces({ MediaType.APPLICATION_JSON })
    // Return the Delegate services registered in Eureka server (Used for debug)
    public Response eureka() {
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        return Response.status(Response.Status.OK).entity(endpointList).build();
    }
    
    /***********************************   indexing-service/item/fields   ***********************************/
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/item/fields")
    public Response federatedGetItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
    	logger.info("called federated get item fields");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	HashMap<ServiceEndpoint, String> resultList = httpHelper.sendGetRequestToAllDelegates(IndexingServiceHelper.getItemFieldsLocalPath, queryParams);
    	List<Map<String, Object>> aggregatedResults = indexingServiceHelper.mergeGetResponsesByFieldName(resultList);
    	
    	return Response.status(Response.Status.OK)
    				   .type(MediaType.APPLICATION_JSON)
    				   .entity(aggregatedResults)
    				   .build();
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
    @GET
    @Path("/item/fields/local")
    public Response getItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("fieldName", fieldName);
        URI uri = httpHelper.buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.getItemFieldsPath, queryParams);
        
        return httpHelper.forwardGetRequest(IndexingServiceHelper.getItemFieldsLocalPath, uri.toString(), null, frontendServiceUrl);
    }
    
    /***********************************   indexing-service/item/fields - END   ***********************************/
    

    /***********************************   indexing-service/party/fields   ***********************************/
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/party/fields")
    public Response federatedGetPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
    	logger.info("called federated get party fields");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	HashMap<ServiceEndpoint, String> resultList = httpHelper.sendGetRequestToAllDelegates(IndexingServiceHelper.getPartyFieldsLocalPath, queryParams);
    	List<Map<String, Object>> aggregatedResults = indexingServiceHelper.mergeGetResponsesByFieldName(resultList);
    	
    	return Response.status(Response.Status.OK)
    				   .type(MediaType.APPLICATION_JSON)
    				   .entity(aggregatedResults)
    				   .build();
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
    @GET
    @Path("/party/fields/local")
    public Response getPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("fieldName", fieldName);
        URI uri = httpHelper.buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.getPartyFieldsPath, queryParams);
        
        return httpHelper.forwardGetRequest(IndexingServiceHelper.getPartyFieldsLocalPath, uri.toString(), null, frontendServiceUrl);
    }
    
    /***********************************   indexing-service/party/fields - END   ***********************************/
    
    
    /***********************************   indexing-service/item/search   ***********************************/   
     /* @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonParseException 
    */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/item/search")
    public Response federatedPostItemSearch(Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated post item search");
    	//initialize result from the request body
    	IndexingServiceResult indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 
    																			Integer.parseInt(body.get("start").toString())); 
    	HashMap<ServiceEndpoint, String> resultList = indexingServiceHelper.getPostItemSearchAggregatedResults(body);
    	
    	for (ServiceEndpoint endpoint : resultList.keySet()) {
    		String results = resultList.get(endpoint);
    		indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getId().equals(eurekaHandler.getId()));
    	}
    	return Response.status(Response.Status.OK)
    								   .type(MediaType.APPLICATION_JSON)
    								   .entity(indexingServiceResult.getFinalResult())
    								   .build();
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/item/search/local")
    public Response postItemSearch(Map<String, Object> body) {
    	// if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
    	Set<String> localFieldNames = indexingServiceHelper.getLocalFieldNamesFromIndexingSerivce(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.getItemFieldsPath);
    	if (indexingServiceHelper.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	indexingServiceHelper.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
        URI uri = httpHelper.buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.postItemSearchPath, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return httpHelper.forwardPostRequest(IndexingServiceHelper.postItemSearchLocalPath, uri.toString(), body, headers, frontendServiceUrl);
    }
    
    /***********************************   indexing-service/item/search - END   ***********************************/
    
    
    /***********************************   indexing-service/party/search   ***********************************/
     /* @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonParseException 
    */
	@POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/party/search")
    public Response federatedPostPartySearch(Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated post party search");
    	List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
    	//initialize result from the request body
    	IndexingServiceResult indexingServiceResult;
    	if (body.get("start") != null) {
    	indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 
														  Integer.parseInt(body.get("start").toString()));
    	}
    	else {
    		indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 0);
    	}
    	
    	HashMap<ServiceEndpoint, String> resultList = httpHelper.sendPostRequestToAllDelegates(endpointList, IndexingServiceHelper.postPartySearchLocalPath, body);
    	
    	for (ServiceEndpoint endpoint : resultList.keySet()) {
    		String results = resultList.get(endpoint);
    		indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getId().equals(eurekaHandler.getId()));
    	}
    	return Response.status(Response.Status.OK)
    								   .type(MediaType.APPLICATION_JSON)
    								   .entity(indexingServiceResult.getFinalResult())
    								   .build();
    	
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
	@POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/party/search/local")
    public Response postPartySearch(Map<String, Object> body) {
    	// if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
    	Set<String> localFieldNames = indexingServiceHelper.getLocalFieldNamesFromIndexingSerivce(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.getPartyFieldsPath);
    	if (indexingServiceHelper.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	indexingServiceHelper.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
    	URI uri = httpHelper.buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+IndexingServiceHelper.postPartySearchPath, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return httpHelper.forwardPostRequest(IndexingServiceHelper.postPartySearchLocalPath, uri.toString(), body, headers, frontendServiceUrl);
    }
    
    /***********************************   indexing-service/party/search - END   ***********************************/
}
