package eu.nimble.service.delegate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import eu.nimble.service.delegate.catalog.CatalogHandler;
import eu.nimble.service.delegate.eureka.EurekaHandler;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;
import eu.nimble.service.delegate.http.HttpHelper;
import eu.nimble.service.delegate.identity.IdentityHandler;
import eu.nimble.service.delegate.indexing.IndexingHandler;
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

    private static String FRONTEND_URL = "FRONTEND_URL";
    private static String _frontendServiceUrl;
    
    private static EurekaHandler _eurekaHandler;
    private static HttpHelper _httpHelper;
    
    private static IndexingHandler _indexingHandler;
    private static IdentityHandler _identityHandler;
    private static CatalogHandler _catalogHandler;
    /***********************************   Servlet Context   ***********************************/
    
    public void contextInitialized(ServletContextEvent arg0) 
    {
    	try {
    		_frontendServiceUrl = System.getenv(FRONTEND_URL);
    	}
    	catch (Exception ex) {
    		logger.error("service env vars are not set as expected");
    	}
    	
        logger.info("Delegate service is being initialized with frontend service param = " + _frontendServiceUrl);
        
        _eurekaHandler = new EurekaHandler();
        if (!_eurekaHandler.initEureka()) {
            logger.error("Failed to initialize Eureka client");
            return;
        }
        
        _httpHelper = new HttpHelper(_eurekaHandler);
        _indexingHandler = new IndexingHandler(_httpHelper, _eurekaHandler);
        _identityHandler = new IdentityHandler(_httpHelper);
        _catalogHandler = new CatalogHandler(_httpHelper, _eurekaHandler);
        
        logger.info("Delegate service has been initialized");
    }

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
    	_eurekaHandler.destroy();
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
        List<ServiceEndpoint> endpointList = _eurekaHandler.getEndpointsFromEureka();
        return Response.status(Response.Status.OK).entity(endpointList).build();
    }

    /***************************************************   INDEXING SERVICE   ***************************************************/
    
    /***********************************   indexing-service/item/fields   ***********************************/
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/item/fields")
    public Response federatedGetItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
    	logger.info("called federated get item fields (indexing service call)");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendGetRequestToAllDelegates(IndexingHandler.GET_ITEM_FIELDS_LOCAL_PATH, queryParams);
    	List<Map<String, Object>> aggregatedResults = _indexingHandler.mergeGetResponsesByFieldName(resultList);
    	
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
        URI uri = _httpHelper.buildUri(IndexingHandler.BaseUrl, IndexingHandler.Port, IndexingHandler.PathPrefix+IndexingHandler.GET_ITEM_FIELDS_PATH, queryParams);
        
        return _httpHelper.forwardGetRequest(IndexingHandler.GET_ITEM_FIELDS_LOCAL_PATH, uri.toString(), null, _frontendServiceUrl);
    }
    
    /***********************************   indexing-service/item/fields - END   ***********************************/
    

    /***********************************   indexing-service/party/fields   ***********************************/
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/party/fields")
    public Response federatedGetPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) {
    	logger.info("called federated get party fields (indexing service call)");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendGetRequestToAllDelegates(IndexingHandler.GET_PARTY_FIELDS_LOCAL_PATH, queryParams);
    	List<Map<String, Object>> aggregatedResults = _indexingHandler.mergeGetResponsesByFieldName(resultList);
    	
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
        URI uri = _httpHelper.buildUri(IndexingHandler.BaseUrl, IndexingHandler.Port, IndexingHandler.PathPrefix+IndexingHandler.GET_PARTY_FIELDS_PATH, queryParams);
        
        return _httpHelper.forwardGetRequest(IndexingHandler.GET_PARTY_FIELDS_LOCAL_PATH, uri.toString(), null, _frontendServiceUrl);
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
    	logger.info("called federated post item search (indexing service call)");
    	//initialize result from the request body
    	IndexingServiceResult indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 
    																			Integer.parseInt(body.get("start").toString())); 
    	HashMap<ServiceEndpoint, String> resultList = _indexingHandler.getPostItemSearchAggregatedResults(body);
    	
    	for (ServiceEndpoint endpoint : resultList.keySet()) {
    		String results = resultList.get(endpoint);
    		indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getId().equals(_eurekaHandler.getId()));
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
    	Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(IndexingHandler.PathPrefix+IndexingHandler.GET_ITEM_FIELDS_PATH);
    	if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	_indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
        URI uri = _httpHelper.buildUri(IndexingHandler.BaseUrl, IndexingHandler.Port, IndexingHandler.PathPrefix+IndexingHandler.POST_ITEM_SEARCH_PATH, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return _httpHelper.forwardPostRequest(IndexingHandler.POST_ITEM_SEARCH_LOCAL_PATH, uri.toString(), body, headers, _frontendServiceUrl);
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
    	logger.info("called federated post party search (indexing service call)");
    	List<ServiceEndpoint> endpointList = _eurekaHandler.getEndpointsFromEureka();
    	//initialize result from the request body
    	IndexingServiceResult indexingServiceResult;
    	if (body.get("start") != null) {
    		indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 
														  	  Integer.parseInt(body.get("start").toString()));
    	}
    	else {
    		indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 0);
    	}
    	
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendPostRequestToAllDelegates(endpointList, IndexingHandler.POST_PARTY_SEARCH_LOCAL_PATH, body);
    	
    	for (ServiceEndpoint endpoint : resultList.keySet()) {
    		String results = resultList.get(endpoint);
    		indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getId().equals(_eurekaHandler.getId()));
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
    	Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(IndexingHandler.PathPrefix+IndexingHandler.GET_PARTY_FIELDS_PATH);
    	if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	_indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	 
    	URI uri = _httpHelper.buildUri(IndexingHandler.BaseUrl, IndexingHandler.Port, IndexingHandler.PathPrefix+IndexingHandler.POST_PARTY_SEARCH_PATH, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return _httpHelper.forwardPostRequest(IndexingHandler.POST_PARTY_SEARCH_LOCAL_PATH, uri.toString(), body, headers, _frontendServiceUrl);
    }
    
    /***********************************   indexing-service/party/search - END   ***********************************/
	
	/************************************************   INDEXING SERVICE - END   ************************************************/
	
	/***************************************************   CATALOG SERVICE   ***************************************************/
	
	/***********************************   catalog-service/binary-contents   ***********************************/
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-contents")
    public Response getBinaryContents(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) {
    	logger.info("called federated get binary contents (catalog service call)");
    	// validation check of the authorization header
    	if (_identityHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	// TODO replace authorization header to the federation delegate access token
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (uris != null && !uris.isEmpty()) {
    		queryParams.put("uris", uris);
        }
    	logger.info("query params: " + queryParams.toString());

    	String targetNimbleInstnaceName = headers.getHeaderString("nimbleInstanceName");
    	ServiceEndpoint nimbleInfo = _eurekaHandler.getEndpointByAppName(targetNimbleInstnaceName);
    	URI targetUri = _httpHelper.buildUri(nimbleInfo.getHostName(), nimbleInfo.getPort(), CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, queryParams);
    	
    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
    	headersToSend.add(HttpHeaders.AUTHORIZATION, "delegate access token in the federation identity service");
    	
    	return _httpHelper.sendGetRequest(targetUri, headersToSend);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
    @GET
    @Path("/binary-contents/local")
    public Response getBinaryContentsLocal(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) {
    	// TODO validate delegate access token
    	// TODO replace access token with the local access token of the delegate service
    	
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("uris", uris);
        URI uri = _httpHelper.buildUri(CatalogHandler.BaseUrl, CatalogHandler.Port, CatalogHandler.PathPrefix+CatalogHandler.GET_BINARY_CONTENTS_PATH, queryParams);
        
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, uri.toString(), null, _frontendServiceUrl);
    }
    
    /***********************************   catalog-service/binary-contents - END   ***********************************/
    
    /************************************************   CATALOG SERVICE - END   ************************************************/
}
