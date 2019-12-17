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
import javax.ws.rs.PathParam;
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
import java.util.LinkedList;
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

    // frontend service
    private static String _frontendServiceUrl;
    private static String FRONTEND_URL = "FRONTEND_URL";
    // identity service of local nimble
    private static String IDENTITY_LOCAL_SERVICE_URL = "IDENTITY_LOCAL_SERVICE_BASE_URL";
    private static String IDENTITY_LOCAL_SERVICE_PORT = "IDENTITY_LOCAL_SERVICE_PORT";
    private static String DELEGATE_LOCAL_USERNAME = "DELEGATE_LOCAL_USERNAME";
    private static String DELEGATE_LOCAL_PASSWORD = "DELEGATE_LOCAL_PASSWORD";
    // identity service of the federation
    private static String IDENTITY_FEDERATION_SERVICE_URL = "IDENTITY_FEDERATION_SERVICE_BASE_URL";
    private static String IDENTITY_FEDERATION_SERVICE_PORT = "IDENTITY_FEDERATION_SERVICE_PORT";
    private static String DELEGATE_FEDERATED_USERNAME = "DELEGATE_FEDERATED_USERNAME";
    private static String DELEGATE_FEDERATED_PASSWORD = "DELEGATE_FEDERATED_PASSWORD";
    
    private static EurekaHandler _eurekaHandler;
    private static HttpHelper _httpHelper;
    
    private static IdentityHandler _identityLocalHandler;
    private static IdentityHandler _identityFederationHandler;
    private static IndexingHandler _indexingHandler;
    private static CatalogHandler _catalogHandler;

    /***********************************   Servlet Context   ***********************************/
    public void contextInitialized(ServletContextEvent arg0) 
    {
        _eurekaHandler = new EurekaHandler();
        if (!_eurekaHandler.initEureka()) {
            logger.error("Failed to initialize Eureka client");
            return;
        }
        _httpHelper = new HttpHelper(_eurekaHandler);
        
        try {
    		_frontendServiceUrl = System.getenv(FRONTEND_URL);
    		logger.info("Delegate service is being initialized with frontend service param = " + _frontendServiceUrl);
    		// local identity service
    		String identityBaseUrl = System.getenv(IDENTITY_LOCAL_SERVICE_URL);
    		int identityPort = -1;
			try {
				identityPort = Integer.parseInt(System.getenv(IDENTITY_LOCAL_SERVICE_PORT));
			} catch (Exception ex) {}
			String[] identityUrlParts = identityBaseUrl.split("/");
			String identityPrefix = "";
			if (identityUrlParts.length > 1) {
				identityBaseUrl = identityUrlParts[0];
				identityPrefix = "/"+String.join("/", Arrays.copyOfRange(identityUrlParts, 1, identityUrlParts.length));
			}
			String username = System.getenv(DELEGATE_LOCAL_USERNAME);
			String password = System.getenv(DELEGATE_LOCAL_PASSWORD);
			
			_identityLocalHandler = new IdentityHandler(_httpHelper, identityBaseUrl, identityPort, identityPrefix, username, password);
			
			// federation identity service
			identityBaseUrl = System.getenv(IDENTITY_FEDERATION_SERVICE_URL);
    		identityPort = -1;
			try {
				identityPort = Integer.parseInt(System.getenv(IDENTITY_FEDERATION_SERVICE_PORT));
			} catch (Exception ex) {}
			identityUrlParts = identityBaseUrl.split("/");
			identityPrefix = "";
			if (identityUrlParts.length > 1) {
				identityBaseUrl = identityUrlParts[0];
				identityPrefix = "/"+String.join("/", Arrays.copyOfRange(identityUrlParts, 1, identityUrlParts.length));
			}
			username = System.getenv(DELEGATE_FEDERATED_USERNAME);
			password = System.getenv(DELEGATE_FEDERATED_PASSWORD);
			
			_identityFederationHandler = new IdentityHandler(_httpHelper, identityBaseUrl, identityPort, identityPrefix, username, password);
    	}
    	catch (Exception ex) {
    		logger.error("service env vars are not set as expected");
    		return;
    	}
        
        _indexingHandler = new IndexingHandler(_httpHelper, _eurekaHandler);
        _catalogHandler = new CatalogHandler();
        
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
    public Response federatedGetItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get item fields (indexing service call)");
    	// validation check of the authorization header in the local identity service
    	if (_identityLocalHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
    	
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendGetRequestToAllDelegates(IndexingHandler.GET_ITEM_FIELDS_LOCAL_PATH, headersToSend, queryParams);
    	List<Map<String, Object>> aggregatedResults = _indexingHandler.mergeGetResponsesByFieldName(resultList);
    	
    	return Response.status(Response.Status.OK)
    				   .type(MediaType.APPLICATION_JSON)
    				   .entity(aggregatedResults)
    				   .build();
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/item/fields/local")
    public Response getItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("fieldName", fieldName);
        URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.GET_ITEM_FIELDS_PATH, queryParams);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardGetRequest(IndexingHandler.GET_ITEM_FIELDS_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   indexing-service/item/fields - END   ***********************************/

    /***********************************   indexing-service/party/fields   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/party/fields")
    public Response federatedGetPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get party fields (indexing service call)");
    	if (_identityLocalHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (fieldName != null && !fieldName.isEmpty()) {
    		queryParams.put("fieldName", fieldName);
        }
    	logger.info("query params: " + queryParams.toString());
    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
    	headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
        
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendGetRequestToAllDelegates(IndexingHandler.GET_PARTY_FIELDS_LOCAL_PATH, headersToSend, queryParams);
    	List<Map<String, Object>> aggregatedResults = _indexingHandler.mergeGetResponsesByFieldName(resultList);
    	
    	return Response.status(Response.Status.OK)
    				   .type(MediaType.APPLICATION_JSON)
    				   .entity(aggregatedResults)
    				   .build();
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/party/fields/local")
    public Response getPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("fieldName", fieldName);
        URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.GET_PARTY_FIELDS_PATH, queryParams);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardGetRequest(IndexingHandler.GET_PARTY_FIELDS_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   indexing-service/party/fields - END   ***********************************/
    
    /***********************************   indexing-service/item/search   ***********************************/   
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/item/search")
    public Response federatedPostItemSearch(@Context HttpHeaders headers, Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated post item search (indexing service call)");
    	if (_identityLocalHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	//initialize result from the request body
    	IndexingServiceResult indexingServiceResult = new IndexingServiceResult(Integer.parseInt(body.get("rows").toString()), 
    																			Integer.parseInt(body.get("start").toString())); 
    	
    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
        
    	HashMap<ServiceEndpoint, String> resultList = _indexingHandler.getPostItemSearchAggregatedResults(headersToSend, body);
    	
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
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/item/search/local")
    public Response postItemSearch(@Context HttpHeaders headers, Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	// if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
    	Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(_indexingHandler.PathPrefix+IndexingHandler.GET_ITEM_FIELDS_PATH);
    	if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	_indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
        URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.POST_ITEM_SEARCH_PATH, null);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add("Content-Type", "application/json");
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardPostRequest(IndexingHandler.POST_ITEM_SEARCH_LOCAL_PATH, uri.toString(), body, headersToSend, _frontendServiceUrl);
    }
    /***********************************   indexing-service/item/search - END   ***********************************/
    
    /***********************************   indexing-service/party/search   ***********************************/
	@POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/party/search")
    public Response federatedPostPartySearch(@Context HttpHeaders headers, Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated post party search (indexing service call)");
    	if (_identityLocalHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
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
    	
    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
    	
    	HashMap<ServiceEndpoint, String> resultList = _httpHelper.sendPostRequestToAllDelegates(endpointList, IndexingHandler.POST_PARTY_SEARCH_LOCAL_PATH, headersToSend, body);
    	
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
	@POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/party/search/local")
    public Response postPartySearch(@Context HttpHeaders headers, Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
		if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	// if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
    	Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(_indexingHandler.PathPrefix+IndexingHandler.GET_PARTY_FIELDS_PATH);
    	if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	_indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	 
    	URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.POST_PARTY_SEARCH_PATH, null);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add("Content-Type", "application/json");
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardPostRequest(IndexingHandler.POST_PARTY_SEARCH_LOCAL_PATH, uri.toString(), body, headersToSend, _frontendServiceUrl);
    }
    /***********************************   indexing-service/party/search - END   ***********************************/
	
	/************************************************   INDEXING SERVICE - END   ************************************************/
	
	/***************************************************   CATALOG SERVICE   ***************************************************/
	
	/***********************************   catalog-service/{standard}/{uuid}   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{standard}/{uuid}") 
    public Response getCatalog(@PathParam("standard") String standard, @PathParam("uuid") String uuid, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get catalog (catalog service call)");
    	return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(CatalogHandler.GET_CATALOG_LOCAL_PATH, standard, uuid), null);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("//catalogue/{standard}/{uuid}/local")
    public Response getCatalogLocal(@PathParam("standard") String standard, @PathParam("uuid") String uuid, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        URI uri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, String.format(_catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_PATH, standard, uuid), null);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
    	
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   catalog-service/{standard}/{uuid} - END   ***********************************/
	
	/***********************************   catalog-service/catalogueline/{hjid}   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogueline/{hjid}") 
    public Response getCatalogLineByHjid(@PathParam("hjid") long hjid, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get catalog line by hjid (catalog service call)");
    	return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(CatalogHandler.GET_CATALOG_LINE_BY_HJID_LOCAL_PATH, hjid), null);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/catalogueline/{hjid}/local")
    public Response getCatalogLineByHjidLocal(@PathParam("hjid") long hjid, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        URI uri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, String.format(_catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_LINE_BY_HJID_PATH, hjid), null);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
    	
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LINE_BY_HJID_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   catalog-service/catalogueline/{hjid} - END   ***********************************/
	
	/******************************   catalog-service/catalogue/{catalogueUuid}/catalogueline/{lineId}   ******************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{catalogueUuid}/catalogueline/{lineId}") 
    public Response getCatalogLine(@PathParam("catalogueUuid") String catalogueUuid, @PathParam("lineId") String lineId, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get catalog line (catalog service call)");
    	return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(CatalogHandler.GET_CATALOG_LINE_LOCAL_PATH, catalogueUuid, lineId), null);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/catalogue/{catalogueUuid}/catalogueline/{lineId}/local")
    public Response getCatalogLineLocal(@PathParam("catalogueUuid") String catalogueUuid, @PathParam("lineId") String lineId, @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        URI uri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, String.format(_catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_LINE_PATH, catalogueUuid, lineId), null);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
    	
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LINE_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /**************************   catalog-service/catalogue/{catalogueUuid}/catalogueline/{lineId} - END   **************************/
    
    /****************************************   catalog-service/binary-content   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-content") 
    public Response getBinaryContent(@Context HttpHeaders headers, @QueryParam("uri") String uri) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get binary content (catalog service call)");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (uri != null) {
    		List<String> list = new LinkedList<String>();
    		list.add(uri);
    		queryParams.put("uri", list);
        }
    	return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), CatalogHandler.GET_BINARY_CONTENT_LOCAL_PATH, queryParams);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/binary-content/local")
    public Response getBinaryContentLocal(@Context HttpHeaders headers, @QueryParam("uri") String uri) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (uri != null) {
    		List<String> list = new LinkedList<String>();
    		list.add(uri);
    		queryParams.put("uri", list);
    	}
        URI catalogServiceUri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, _catalogHandler.PathPrefix+CatalogHandler.GET_BINARY_CONTENT_PATH, queryParams);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_BINARY_CONTENT_LOCAL_PATH, catalogServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   catalog-service/binary-content - END   ************************************/
    
    /****************************************   catalog-service/catalogue/{catalogueUuid}/cataloguelines   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{catalogueUuid}/cataloguelines") 
    public Response getCatalogLines(@PathParam("catalogueUuid") String catalogueUuid, @Context HttpHeaders headers, @QueryParam("lineIds") List<String> lineIds) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get catalog lines (catalog service call)");
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (lineIds != null) {
    		queryParams.put("lineIds", lineIds);
        }
    	return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(CatalogHandler.GET_CATALOG_LINES_LOCAL_PATH, catalogueUuid), queryParams);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/catalogue/{catalogueUuid}/cataloguelines/local")
    public Response getCatalogLinesLocal(@PathParam("catalogueUuid") String catalogueUuid, @Context HttpHeaders headers, @QueryParam("lineIds") List<String> lineIds) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (lineIds != null) {
    		queryParams.put("lineIds", lineIds);
        }
        URI catalogServiceUri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, String.format(_catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_LINES_PATH, catalogueUuid), queryParams);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LINES_LOCAL_PATH, catalogServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   catalog-service/catalogue/{catalogueUuid}/cataloguelines - END   ************************************/
    
	/***********************************   catalog-service/binary-contents   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-contents")
    public Response getBinaryContents(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) throws JsonParseException, JsonMappingException, IOException {
    	logger.info("called federated get binary contents (catalog service call)");
    	// validation check of the authorization header in the local identity service
    	if (_identityLocalHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
    	if (uris != null && !uris.isEmpty()) {
    		queryParams.put("uris", uris);
        }
    	logger.info("query params: " + queryParams.toString());

    	MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
        
        HashMap<ServiceEndpoint, String> delegatesResponse = _httpHelper.sendGetRequestToAllDelegates(CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, headersToSend, queryParams);
    	
        // TODO aggregate results
        return _catalogHandler.buildResponseFromSingleDelegate(delegatesResponse);
    }
    
    // a REST call that should be used between delegates. 
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/binary-contents/local")
    public Response getBinaryContentsLocal(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) throws JsonParseException, JsonMappingException, IOException {
    	if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        if (uris != null && !uris.isEmpty()) {
    		queryParams.put("uris", uris);
        }
        URI uri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, _catalogHandler.PathPrefix+CatalogHandler.GET_BINARY_CONTENTS_PATH, queryParams);
        
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
    	
        return _httpHelper.forwardGetRequest(CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   catalog-service/binary-contents - END   ***********************************/
    
    /***********************************   catalog-service - helper function   ***********************************/
    private Response catalogServiceCallWrapper(String userAccessToken, String pathToSendRequest, HashMap<String, List<String>> queryParams) throws JsonParseException, JsonMappingException, IOException {
    	// validation check of the authorization header in the local identity service
    	if (_identityLocalHandler.userExist(userAccessToken) == false) {
    		return Response.status(Response.Status.UNAUTHORIZED).build();
    	}
    	if (queryParams != null) {
    		logger.info("query params: " + queryParams.toString());
    	}
    	// replace the authorization header to the federation identity of the delegate service
    	MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
    	
        HashMap<ServiceEndpoint, String> delegatesResponse = _httpHelper.sendGetRequestToAllDelegates(pathToSendRequest, headers, queryParams);
    	return _catalogHandler.buildResponseFromSingleDelegate(delegatesResponse);
    }
    /***********************************   catalog-service - helper function - END   ***********************************/
    
    /************************************************   CATALOG SERVICE - END   ************************************************/
}
