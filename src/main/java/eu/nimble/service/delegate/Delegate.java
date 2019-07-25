package eu.nimble.service.delegate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.PathParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    private static final int REQ_TIMEOUT_SEC = 3;

    private static Logger logger = LogManager.getLogger(Delegate.class);

    private static EurekaHandler eurekaHandler = new EurekaHandler();
    
    private static Client httpClient;
    
    private static String frontendServiceUrl;
    private static String indexingServiceBaseUrl;
    private static String indexingServicePathPrefix;
    private static int indexingServicePort;
    private static String catalogueServiceBaseUrl;
    private static String catalogueServicePathPrefix;
    private static int catalogueServicePort;

    private static String getItemFieldsPath = "/item/fields";
    private static String getItemFieldsLocalPath = "/item/fields/local";
    private static String getPartyFieldsPath = "/party/fields";
    private static String getPartyFieldsLocalPath = "/party/fields/local";
    private static String postItemSearchPath = "/item/search";
    private static String postItemSearchLocalPath = "/item/search/local";
    private static String postPartySearchPath = "/party/search";
    private static String postPartySearchLocalPath = "/party/search/local";

    /** Catalogue Service Paths **/
    private static String getCatalogueLinePath = "/catalogue/%s/catalogueline/%s";
    private static String getCatalogueLineLocalPath = "/catalogue/%s/catalogueline/%s/local";
    private static String getCatalogueLinesPath = "/catalogue/%s/cataloguelines";
    private static String getCatalogueLinesLocalPath = "/catalogue/%s/cataloguelines/local";
    private static String getCatalogueLineByHjidPath = "/catalogueline/%s";
    private static String getCatalogueLineByHjidLocalPath = "/catalogueline/%s/local";
    private static String getCataloguePath = "/catalogue/%s/%s";
    private static String getCataloguePathLocal = "/catalogue/%s/%s/local";
    private static String getBinaryContentPath = "/binary-content";
    private static String getBinaryContentLocalPath = "/binary-content/local";
    private static String getBinaryContentsPath = "/binary-contents";
    private static String getBinaryContentsLocalPath = "/binary-contents/local";
    private static String getBase64BinaryContentPath = "/binary-content/raw";
    private static String getBase64BinaryContentLocalPath = "/binary-content/raw/local";

    /** Catalogue Bearer Token **/
    private static String catalogueBearerToken = "dummyTokenHere";

    private static ObjectMapper mapper = new ObjectMapper();
    private static JsonParser jsonParser = new JsonParser();
    
    /***********************************   Servlet Context   ***********************************/
    
    public void contextInitialized(ServletContextEvent arg0) 
    {
        try {
            frontendServiceUrl = System.getenv("FRONTEND_URL");
            indexingServiceBaseUrl = System.getenv("INDEXING_SERVICE_URL");
            catalogueServiceBaseUrl = System.getenv("CATALOGUE_SERVICE_URL");
            try {
                indexingServicePort = Integer.parseInt(System.getenv("INDEXING_SERVICE_PORT"));
            }
            catch (Exception ex) {
                indexingServicePort = -1;
            }
            try {
                catalogueServicePort = Integer.parseInt(System.getenv("CATALOGUE_SERVICE_PORT"));
            }
            catch (Exception e){
                catalogueServicePort = -1;
            }
            String[] indexingServiceUrlParts = indexingServiceBaseUrl.split("/");
            if (indexingServiceUrlParts.length > 1) {
                indexingServiceBaseUrl = indexingServiceUrlParts[0];
                indexingServicePathPrefix = "/"+String.join("/", Arrays.copyOfRange(indexingServiceUrlParts, 1, indexingServiceUrlParts.length));
            }
            else {
                indexingServicePathPrefix = "";
            }
            String[] catalogueServiceUrlParts = catalogueServiceBaseUrl.split("/");
            if (catalogueServiceUrlParts.length > 1) {
                catalogueServiceBaseUrl = catalogueServiceUrlParts[0];
                catalogueServicePathPrefix = "/"+String.join("/", Arrays.copyOfRange(catalogueServiceUrlParts, 1, catalogueServiceUrlParts.length));
            }
            else {
                catalogueServicePathPrefix = "";
            }
        }
        catch (Exception ex) {
            logger.warn("env vars are not set as expected");
        }

        logger.info("Delegate service is being initialized with frontend service param = " + frontendServiceUrl
                + ", catalogue service base url = " + catalogueServiceBaseUrl
                + ", catalogue service prefix = " + catalogueServicePathPrefix
                + ", catalogue service port = " + catalogueServicePort
                + ", indexing service base url = " + indexingServiceBaseUrl
                + ", indexing service prefix = " + indexingServicePathPrefix
                + ", indexing service port = " + indexingServicePort + "...");

        httpClient = ClientBuilder.newClient();

        if (!eurekaHandler.initEureka()) {
            logger.error("Failed to initialize Eureka client");
            return;
        }
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
    	HashMap<ServiceEndpoint, String> resultList = sendGetRequestToAllServices(getItemFieldsLocalPath, queryParams);
    	List<Map<String, Object>> aggregatedResults = mergeGetResponsesByFieldName(resultList);
    	
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
        URI uri = buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+getItemFieldsPath, queryParams);
        logger.info("got a request to endpoint " + getItemFieldsLocalPath + ", forwarding to " + uri.toString());
        
        Response response = httpClient.target(uri.toString()).request().get();
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
        	String data = response.readEntity(String.class);
            return Response.status(Status.OK)
            			   .entity(data)
            			   .type(MediaType.APPLICATION_JSON)
            			   .header("frontendServiceUrl", frontendServiceUrl)
            			   .build();
        }
        else {
        	return response;
        }
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
    	HashMap<ServiceEndpoint, String> resultList = sendGetRequestToAllServices(getPartyFieldsLocalPath, queryParams);
    	List<Map<String, Object>> aggregatedResults = mergeGetResponsesByFieldName(resultList);
    	
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
        URI uri = buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+getPartyFieldsPath, queryParams);
        logger.info("got a request to endpoint " + getPartyFieldsLocalPath + ", forwarding to " + uri.toString());
        
        Response response = httpClient.target(uri.toString()).request().get();
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
        	String data = response.readEntity(String.class);
            return Response.status(Status.OK)
            			   .entity(data)
            			   .type(MediaType.APPLICATION_JSON)
            			   .header("frontendServiceUrl", frontendServiceUrl)
            			   .build();
        }
        else {
        	return response;
        }
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
    	HashMap<ServiceEndpoint, String> resultList = getPostItemSearchAggregatedResults(body);
    	
    	for (ServiceEndpoint endpoint : resultList.keySet()) {
    		String results = resultList.get(endpoint);
    		indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getId().equals(eurekaHandler.getId()));
    	}
    	
    	return Response.status(Response.Status.OK)
    								   .type(MediaType.APPLICATION_JSON)
    								   .entity(indexingServiceResult.getFinalResult())
    								   .build();
    }
    
    @SuppressWarnings("unchecked")
	private HashMap<ServiceEndpoint, String> getPostItemSearchAggregatedResults(Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
    	int requestedPageSize = Integer.parseInt(body.get("rows").toString()); // save it before manipulating
    	// manipulate body in order to get results from all delegates.
    	List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
    	body.put("rows", 0); // send dummy request just to get totalElements fields from all delegates
    	HashMap<ServiceEndpoint, String> dummyResultList = sendPostRequestToAllServices(endpointList, postItemSearchLocalPath, body);
    	List<ServiceEndpoint> endpointsToRemove = new LinkedList<ServiceEndpoint>();
    	for (ServiceEndpoint endpoint : dummyResultList.keySet()) {
    		String result = dummyResultList.get(endpoint);
    		if (result == null || result.isEmpty()) {
    			endpointsToRemove.add(endpoint);
    		}
    	}
    	for (ServiceEndpoint endpoint : endpointsToRemove) {
    		dummyResultList.remove(endpoint);
    		endpointList.remove(endpoint);
    	}
    	
    	int sumTotalElements = 0;
    	final LinkedHashMap<ServiceEndpoint, Integer> totalElementPerEndpoint = new LinkedHashMap<ServiceEndpoint, Integer>();
    	for (Entry<ServiceEndpoint, String> entry : dummyResultList.entrySet()) {
    		Map<String, Object> json = mapper.readValue(entry.getValue(), Map.class);
    		int totalElementForEndpoint = Integer.parseInt(json.get("totalElements").toString());
    		totalElementPerEndpoint.put(entry.getKey(), totalElementForEndpoint);
    		sumTotalElements += totalElementForEndpoint;
    	}
    	if (sumTotalElements <= requestedPageSize || requestedPageSize == 0 || endpointList.size()==1) {
    		body.put("rows", requestedPageSize); 
    		return sendPostRequestToAllServices(endpointList, postItemSearchLocalPath, body);
    	}
    	// else, we need to decide how many results we want from each delegate
    	// TODO work on this logic!
    	logger.info("we need to decide how many results to get from each delegate");
    	logger.info("sum of total elements = " + sumTotalElements);
    	int numOfRowsAggregated = 0;
    	HashMap<ServiceEndpoint, String> aggregatedResults = new LinkedHashMap<ServiceEndpoint, String>();
    	for (ServiceEndpoint endpoint : endpointList) {
    		int totalElementOfEndpoint = totalElementPerEndpoint.get(endpoint);
    		logger.info("totalElements of endpoint + " + endpoint.getHostName() + ":" + endpoint.getPort()+ " = " + totalElementOfEndpoint);
    		int endpointRows = Math.min(Math.round(totalElementOfEndpoint/((float)sumTotalElements)*requestedPageSize),(requestedPageSize-numOfRowsAggregated));
    		List<ServiceEndpoint> listForRequest = new LinkedList<ServiceEndpoint>();
    		listForRequest.add(endpoint);
    		body.put("rows", endpointRows); // manipulate body values
    		logger.info("requesting from endpoint " + endpointRows + " rows, body = " + body);
    		aggregatedResults.putAll(sendPostRequestToAllServices(listForRequest, postItemSearchLocalPath, body));
    		numOfRowsAggregated += endpointRows;
    	}
    	
    	return aggregatedResults;
    }
    
    // a REST call that should be used between delegates.
    // the origin delegate sends a request and the target delegate will perform the query locally.
    // TODO add authorization header to make sure the caller is a delegate rather than a human (after adding federation identity service)
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/item/search/local")
    public Response postItemSearch(Map<String, Object> body) {
    	// if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
    	Set<String> localFieldNames = getLocalFieldNamesFromIndexingSerivce(indexingServicePathPrefix+getItemFieldsPath);
    	if (fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
        URI uri = buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+postItemSearchPath, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return forwardPostRequest(postItemSearchLocalPath, uri.toString(), body, headers);
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
    	
    	HashMap<ServiceEndpoint, String> resultList = sendPostRequestToAllServices(endpointList, postPartySearchLocalPath, body);
    	
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
    	Set<String> localFieldNames = getLocalFieldNamesFromIndexingSerivce(indexingServicePathPrefix+getPartyFieldsPath);
    	if (fqListContainNonLocalFieldName(body, localFieldNames)) {
    		return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
    	}
    	// remove from body.facet.field all fieldNames that doesn't exist in local instance 
    	removeNonExistingFieldNamesFromBody(body, localFieldNames);
    	
    	URI uri = buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServicePathPrefix+postPartySearchPath, null);
        
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add("Content-Type", "application/json");
        
        return forwardPostRequest(postPartySearchLocalPath, uri.toString(), body, headers);
    }
    
    /***********************************   indexing-service/party/search - END   ***********************************/

    /***********************************   Catalogue Service **************************************************/

    /***********************************   /catalogue/{catalogueUuid}/catalogueline/{lineId} ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{catalogueUuid}/catalogueline/{lineId}")
    public Response federatedGetCatalogueLine(@Context HttpHeaders headers, @PathParam("catalogueUuid") String catalogueUuid, @PathParam("lineId") String lineId, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get catalogue line");

        HashMap<String, String> queryParams = new HashMap<String, String>();
        String path = String.format(getCatalogueLineLocalPath,catalogueUuid,lineId);
        String result = sendGetRequest(path,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/catalogue/{catalogueUuid}/catalogueline/{lineId}/local")
    public Response getCatalogueLine(@Context HttpHeaders headers, @PathParam("catalogueUuid") String catalogueUuid, @PathParam("lineId") String lineId) {
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        String path = String.format(getCatalogueLinePath,catalogueUuid,lineId);
        path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + path : path;
        URI uri = buildUri(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + String.format(getCatalogueLinePath,catalogueUuid,lineId) + ", forwarding to " + uri.toString());

        return sendGetRequest(uri);
    }

    /***********************************   /catalogue/{catalogueUuid}/catalogueline/{lineId} - END *********************************/

    /***********************************   /catalogue/{catalogueUuid}/cataloguelines ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{catalogueUuid}/cataloguelines")
    public Response federatedGetCatalogueLines(@Context HttpHeaders headers, @PathParam("catalogueUuid") String catalogueUuid,@QueryParam("lineIds") List<String> lineIds, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get catalogue lines");

        String lineIdsQueryParam = "";
        int size = lineIds.size();
        for(int i = 0; i < size; i++){
            if(i == size-1){
                lineIdsQueryParam += lineIds.get(i);
            }
            else{
                lineIdsQueryParam += lineIds.get(i) + ",";
            }
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("lineIds",lineIdsQueryParam);

        String path = String.format(getCatalogueLinesLocalPath,catalogueUuid);
        String result = sendGetRequest(path,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/catalogue/{catalogueUuid}/cataloguelines/local")
    public Response getCatalogueLines(@Context HttpHeaders headers, @PathParam("catalogueUuid") String catalogueUuid,@QueryParam("lineIds") List<String> lineIds) {

        String lineIdsQueryParam = "";
        int size = lineIds.size();
        for(int i = 0; i < size; i++){
            if(i == size-1){
                lineIdsQueryParam += lineIds.get(i);
            }
            else{
                lineIdsQueryParam += lineIds.get(i) + ",";
            }
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("lineIds",lineIdsQueryParam);

        String path = String.format(getCatalogueLinesPath,catalogueUuid);
        path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + path : path;
        URI uri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + String.format(getCatalogueLinesPath,catalogueUuid) + ", forwarding to " + uri.toString());

        return sendGetRequest(uri);
    }

    /***********************************   /catalogue/{catalogueUuid}/cataloguelines - END *********************************/

    /***********************************   /catalogueline/{hjid} ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogueline/{hjid}")
    public Response federatedGetCatalogueLineByHjid(@Context HttpHeaders headers, @PathParam("hjid") Long hjid, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get catalogue line by hjid");


        HashMap<String, String> queryParams = new HashMap<String, String>();


        String path = String.format(getCatalogueLineByHjidLocalPath,hjid);
        String result = sendGetRequest(path,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/catalogueline/{hjid}/local")
    public Response getCatalogueLineByHjid(@Context HttpHeaders headers, @PathParam("hjid") Long hjid) {

        HashMap<String, String> queryParams = new HashMap<String, String>();

        String path = String.format(getCatalogueLineByHjidPath,hjid);
        path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + path : path;
        URI uri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + String.format(getCatalogueLineByHjidPath,hjid) + ", forwarding to " + uri.toString());

        return sendGetRequest(uri);
    }

    /***********************************   /catalogueline/{hjid} - END *********************************/

    /***********************************   /catalogue/{standard}/{uuid} ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/{standard}/{uuid}")
    public Response federatedGetCatalogue(@Context HttpHeaders headers, @PathParam("standard") String standard,@PathParam("uuid") String uuid, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get catalogue line by hjid");


        HashMap<String, String> queryParams = new HashMap<String, String>();

        String path = String.format(getCataloguePathLocal,standard,uuid);
        String result = sendGetRequest(path,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/catalogue/{standard}/{uuid}/local")
    public Response getCatalogue(@Context HttpHeaders headers, @PathParam("standard") String standard,@PathParam("uuid") String uuid) {

        HashMap<String, String> queryParams = new HashMap<String, String>();

        String path = String.format(getCataloguePath,standard,uuid);
        path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + path : path;
        URI uri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + String.format(getCataloguePath,standard,uuid) + ", forwarding to " + uri.toString());

        return sendGetRequest(uri);
    }

    /***********************************   /catalogue/{standard}/{uuid} - END *********************************/

    /***********************************   /binary-content ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-content")
    public Response federatedGetBinaryContent(@Context HttpHeaders headers, @QueryParam("uri") String uri, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get binary content");

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uri",uri);

        String result = sendGetRequest(getBinaryContentLocalPath,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/binary-content/local")
    public Response getBinaryContent(@Context HttpHeaders headers, @QueryParam("uri") String uri) {

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uri",uri);

        String path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + getBinaryContentPath : getBinaryContentPath;
        URI catalogUri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + getBinaryContentPath + ", forwarding to " + catalogUri.toString());

        return sendGetRequest(catalogUri);
    }

    /***********************************   /binary-content - END *********************************/

    /***********************************   /binary-contents ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-contents")
    public Response federatedGetBinaryContents(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get binary contents");

        String urisQueryParam = "";
        int size = uris.size();
        for(int i = 0; i < size; i++){
            if(i == size-1){
                urisQueryParam += uris.get(i);
            }
            else{
                urisQueryParam += uris.get(i) + ",";
            }
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uris",urisQueryParam);

        String result = sendGetRequest(getBinaryContentsLocalPath,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/binary-contents/local")
    public Response getBinaryContents(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) {

        String urisQueryParam = "";
        int size = uris.size();
        for(int i = 0; i < size; i++){
            if(i == size-1){
                urisQueryParam += uris.get(i);
            }
            else{
                urisQueryParam += uris.get(i) + ",";
            }
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uris",urisQueryParam);

        String path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + getBinaryContentsPath : getBinaryContentsPath;
        URI catalogUri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + getBinaryContentsPath + ", forwarding to " + catalogUri.toString());

        return sendGetRequest(catalogUri);
    }

    /***********************************   /binary-contents - END *********************************/

    /***********************************   /binary-content/raw ***************************************/

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-content/raw")
    public Response federatedGetBase64BinaryContent(@Context HttpHeaders headers, @QueryParam("uri") String uri, @QueryParam("delegateId") String delegateId) {
        logger.info("called federated get Base 64 binary content");

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uri",uri);

        String result = sendGetRequest(getBase64BinaryContentLocalPath,queryParams,delegateId);

        return Response.status(Response.Status.OK)
                .type(MediaType.APPLICATION_JSON)
                .entity(result)
                .build();
    }

    @GET
    @Path("/binary-content/raw/local")
    public Response getBase64BinaryContent(@Context HttpHeaders headers, @QueryParam("uri") String uri) {

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uri",uri);

        String path = catalogueServicePathPrefix != null ? catalogueServicePathPrefix + "/" + getBase64BinaryContentPath : getBase64BinaryContentPath;
        URI catalogUri = buildUriWithStringParams(catalogueServiceBaseUrl, catalogueServicePort, path, queryParams);
        logger.info("got a request to endpoint " + getBase64BinaryContentPath + ", forwarding to " + catalogUri.toString());

        return sendGetRequest(catalogUri);
    }

    /***********************************   /binary-content/raw - END *********************************/

    /***********************************   indexing-service extra logic   ***********************************/
    
    // if field name exists in more than one instance, putting the entry just once, ignoring doc count field
    private List<Map<String, Object>> mergeGetResponsesByFieldName(HashMap<ServiceEndpoint, String> resultList) {
    	logger.info("merging results of GET request based on field name");
    	List<Map<String, Object>> aggregatedResults = new LinkedList<Map<String, Object>>();
    	
    	for (String results : resultList.values()) {
    		if (results == null || results.isEmpty()) {
				continue;
			}
    		List<Map<String, Object>> json;
			try {
				json = mapper.readValue(results, mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
				for (int i=0; i<json.size(); ++i) {
					Map<String, Object> jsonObject = json.get(i);
					String key = jsonObject.get("fieldName").toString();
					if (!containsFieldName(aggregatedResults, key)) {
						aggregatedResults.add(jsonObject);
					}
					else {
						logger.info("field name = " + key + " already exist, skipping");
					}
				}
			} catch (IOException e) {
				logger.warn("failed to read response json " + e.getMessage());
			}
    	}
    	return aggregatedResults;
    }
    
	private boolean containsFieldName(List<Map<String, Object>> aggregatedResults, String fieldName) {
    	for (Map<String, Object> jsonObject : aggregatedResults) {
    		String key = jsonObject.get("fieldName").toString();
    		if (key.equals(fieldName)) {
    			return true;
    		}
    	}
    	return false;
    }
    
    private Set<String> getLocalFieldNamesFromIndexingSerivce(String indexingServiceRelativePath) {
    	URI uri = buildUri(indexingServiceBaseUrl, indexingServicePort, indexingServiceRelativePath, null);
        logger.info("sending a request to " + uri.toString() + " in order to clean non existing field names");
        
        Response response = httpClient.target(uri.toString()).request().get();
        if (response.getStatus() >= 400) { // we had an issue, we can't modify the body without any response
       	 logger.warn("get error when calling GET '/item/fields' in indexing service");
       	 return new HashSet<String>();
        }
        
        String data = response.readEntity(String.class);
        Set<String> localFieldNames = new HashSet<String>();
		try {
			List<Map<String, Object>> json = mapper.readValue(data, mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
			for (int i=0; i<json.size(); ++i) {
				Map<String, Object> jsonObject = json.get(i);
				String fieldName = jsonObject.get("fieldName").toString();
				localFieldNames.add(fieldName);
			}
		}
		catch (Exception ex) {
			logger.warn("get error while processing GET '/item/fields' response from indexing service...");
		}
		return localFieldNames;
    }
    
	private boolean fqListContainNonLocalFieldName(Map<String, Object> body, Set<String> localFieldNames) {
    	if (body.get("fq") == null) {
			return false; 
		}
    	try {
    		String fqStr = body.get("fq").toString();
    		fqStr = fqStr.substring(1, fqStr.length()-1).trim();
    		String[] fqList = fqStr.split(",");
    		for (String fq : fqList) {
    			fq = fq.trim();
    			String fqFieldName = fq.split(":")[0];
    			logger.info("checking fq: " + fq + ", fq fieldName = " + fqFieldName);
    			if (fqFieldName != null && !localFieldNames.contains(fqFieldName)) {
    				logger.info("fq field name " + fqFieldName + " doesn't exist in local instance, returns empty result");
    				return true;
    			}
    		}
    	}
    	catch (Exception ex) {
    		logger.warn("error while trying to cast fq field to json");
    	}
    	return false;
    }
    
    // remove from body.facet.fields all field names that doesn't exist in the local indexing service instance
    private void removeNonExistingFieldNamesFromBody(Map<String, Object> body, Set<String> localFieldNames) {
    	if (body.get("facet") == null) {
    		return;
    	}
    	JsonObject facetJsonObject = jsonParser.parse(body.get("facet").toString()).getAsJsonObject();
    	JsonArray fieldJsonObject = facetJsonObject.get("field").getAsJsonArray();
    	
    	List<String> facetFieldNewValue = new LinkedList<String>();
    	
    	for (JsonElement element : fieldJsonObject) {
    		String fieldName = element.getAsString();
    		if (fieldName != null && localFieldNames.contains(fieldName)) {
    			facetFieldNewValue.add(fieldName);
    		}
    	}
    	Map<String, Object> facetField = new HashMap<String, Object>();
    	facetField.put("field", facetFieldNewValue);
    	facetField.put("minCount", facetJsonObject.get("minCount").getAsInt());
    	facetField.put("limit", facetJsonObject.get("limit").getAsInt());
    	body.put("facet", facetField);
    }
    
    /***********************************   indexing-service extra logic - END   ***********************************/
    
    
    /***********************************   Http Requests   ***********************************/

    private URI buildUriWithStringParams(String host, int port, String path, HashMap<String, String> queryParams) {
        // Prepare the destination URL for the request
        UriBuilder uriBuilder = UriBuilder.fromUri("");
        uriBuilder.scheme("http");
        if (queryParams != null) {
            for (Entry<String, String> queryParam : queryParams.entrySet()) {
                uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
            }
        }
        uriBuilder.host(host).path(path);
        if (port > 0) { // in case the request is sent to nginx, no port is needed (will be set to -1)
            uriBuilder.port(port);
        }
        return uriBuilder.build();
    }

    private URI buildUri(String host, int port, String path, HashMap<String, List<String>> queryParams) {
    	// Prepare the destination URL for the request
        UriBuilder uriBuilder = UriBuilder.fromUri("");
        uriBuilder.scheme("http");
        if (queryParams != null) {
        	for (Entry<String, List<String>> queryParam : queryParams.entrySet()) {
        		if (queryParam.getValue() != null && !queryParam.getValue().isEmpty()) {
        			uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
        		}
        	}
        }
        uriBuilder.host(host).path(path);
        if (port > 0) { // in case the request is sent to nginx, no port is needed (will be set to -1)
        	uriBuilder.port(port);
        }
        return uriBuilder.build();
    }
    
    // forward post request
    private Response forwardPostRequest(String from, String to, Map<String, Object> body, MultivaluedMap<String, Object> headers) {
    	logger.info("got a request to endpoint " + from + ", forwarding it to " + to + " with body: " + body.toString());
        
        Response response = httpClient.target(to).request().headers(headers).post(Entity.json(body));
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
        	String data = response.readEntity(String.class);
            return Response.status(Status.OK)
            				.entity(data)
            				.type(MediaType.APPLICATION_JSON)
            				.header("frontendServiceUrl", frontendServiceUrl)
            				.build();
        }
        else {
        	return response;
        }
    }

    private Response sendGetRequest(URI uri){
        Response response = httpClient.target(uri.toString()).request().header("Authorization",catalogueBearerToken).get();
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
            String data = response.readEntity(String.class);
            return Response.status(Status.OK)
                    .entity(data)
                    .type(MediaType.APPLICATION_JSON)
                    .header("frontendServiceUrl", frontendServiceUrl)
                    .build();
        }
        else {
            return response;
        }
    }

    private String sendGetRequest(String urlPath,HashMap<String, String> queryParams, String delegateId){
        // if no delegate is specified, send Get request to the local one
        delegateId = delegateId != null ? delegateId: eurekaHandler.getId();
        logger.info("send get request to delegate {}",delegateId);

        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();

        for (ServiceEndpoint endpoint : endpointList) {
            if(endpoint.getId().contentEquals(delegateId)){
                // Prepare the destination URL
                UriBuilder uriBuilder = UriBuilder.fromUri("");
                uriBuilder.scheme("http");

                // add all query params to the request
                for (Entry<String, String> queryParam : queryParams.entrySet()) {
                    uriBuilder.queryParam(queryParam.getKey(),queryParam.getValue());
                }

                URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

                logger.info("sending the request to " + endpoint.toString() + "...");
                Future<Response> result = httpClient.target(uri.toString()).request().async().get();

                logger.info("got response from " + endpoint.toString());
                try {
                    Response res = result.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                    String data = res.readEntity(String.class);
                    endpoint.setFrontendServiceUrl(res.getHeaderString("frontendServiceUrl"));
                    logger.info("data :" + data);
                    return data;
                } catch(Exception e) {
                    logger.warn("Failed to send get request to eureka endpoint: id: " +  endpoint.getId() +
                            " appName:" + endpoint.getAppName() +
                            " (" + endpoint.getHostName() +
                            ":" + endpoint.getPort() + ") - " +
                            e.getMessage());
                }
            }
        }
        return null;
    }

    // Sends the get request to all the Delegate services which are registered in the Eureka server
    private HashMap<ServiceEndpoint, String> sendGetRequestToAllServices(String urlPath, HashMap<String, List<String>> queryParams) {
    	logger.info("send get requests to all delegates");
    	List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        List<Future<Response>> futureList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : endpointList) {
            // Prepare the destination URL
            UriBuilder uriBuilder = UriBuilder.fromUri("");
            uriBuilder.scheme("http");
            // add all query params to the request
            for (Entry<String, List<String>> queryParam : queryParams.entrySet()) {
            	for (String paramValue : queryParam.getValue()) {
            		uriBuilder.queryParam(queryParam.getKey(), paramValue);
            	}
            }
            URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();
            
            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().async().get();
            futureList.add(result);
        }
        return getResponseListFromAllDelegates(endpointList, futureList);
    }
    
    // Sends the post request to all the Delegate services which are registered in the Eureka server
    private HashMap<ServiceEndpoint, String> sendPostRequestToAllServices(List<ServiceEndpoint> endpointList, String urlPath, Map<String, Object> body) {
    	logger.info("send post requests to all delegates");
        List<Future<Response>> futureList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : endpointList) {
            URI uri = buildUri(endpoint.getHostName(), endpoint.getPort(), urlPath, null);
            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().async().post(Entity.json(body));
            futureList.add(result);
        }
        return getResponseListFromAllDelegates(endpointList, futureList);
    }
    
    // get responses from all Delegate services which are registered in the Eureka server
    private HashMap<ServiceEndpoint, String> getResponseListFromAllDelegates(List<ServiceEndpoint> endpointList, List<Future<Response>> futureList) {
        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            ServiceEndpoint endpoint = endpointList.get(i);
            logger.info("got response from " + endpoint.toString());
            try {
            	Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                String data = res.readEntity(String.class);
                endpoint.setFrontendServiceUrl(res.getHeaderString("frontendServiceUrl"));
                resList.put(endpoint, data);
            } catch(Exception e) {
                logger.warn("Failed to send post request to eureka endpoint: id: " +  endpoint.getId() +
                			" appName:" + endpoint.getAppName() +
                            " (" + endpoint.getHostName() +
                            ":" + endpoint.getPort() + ") - " +
                            e.getMessage());
            }
        }
        logger.info("aggregated results: \n" + resList.toString());
        return resList;
    }
    
    /***********************************   Http Requests - END   ***********************************/
    
    @GET
    @Path("eureka")
    @Produces({ MediaType.APPLICATION_JSON })
    // Return the Delegate services registered in Eureka server (Used for debug)
    public Response eureka() {
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        return Response.status(Response.Status.OK).entity(endpointList).build();
    }
}
