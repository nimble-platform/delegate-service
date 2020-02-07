package eu.nimble.service.delegate;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.nimble.service.delegate.businessprocess.BusinessProcessHandler;
import eu.nimble.service.delegate.businessprocess.MergeOption;
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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import java.net.URLEncoder;
import java.util.*;
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
    private static BusinessProcessHandler _businessProcessHandler;

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
        _businessProcessHandler = new BusinessProcessHandler();

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

    @GET
    @Path("/eureka/app-name")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response eurekaAppName() {
        return Response.status(Response.Status.OK).entity(_eurekaHandler.getAppName()).build();
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
    public Response getItemFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws IOException {
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
    public Response getPartyFields(@Context HttpHeaders headers, @QueryParam("fieldName") List<String> fieldName) throws IOException {
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
            indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getAppName().equals(_eurekaHandler.getAppName()));
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
    public Response postItemSearch(@Context HttpHeaders headers, Map<String, Object> body) throws IOException {
        if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        // if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
        Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(_indexingHandler.PathPrefix+IndexingHandler.GET_ITEM_FIELDS_PATH,headersToSend);
        if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
            return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
        }
        // remove from body.facet.field all fieldNames that doesn't exist in local instance
        _indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);

        URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.POST_ITEM_SEARCH_PATH, null);

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
            indexingServiceResult.addEndpointResponse(endpoint, results, endpoint.getAppName().equals(_eurekaHandler.getAppName()));
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
    public Response postPartySearch(@Context HttpHeaders headers, Map<String, Object> body) throws IOException {
        if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        // if fq list in the request body contains field name that doesn't exist in local instance don't do any search, return empty result
        Set<String> localFieldNames = _indexingHandler.getLocalFieldNamesFromIndexingSerivce(_indexingHandler.PathPrefix+IndexingHandler.GET_PARTY_FIELDS_PATH,headersToSend);
        if (_indexingHandler.fqListContainNonLocalFieldName(body, localFieldNames)) {
            return Response.status(Response.Status.OK).type(MediaType.TEXT_PLAIN).entity("").build();
        }
        // remove from body.facet.field all fieldNames that doesn't exist in local instance
        _indexingHandler.removeNonExistingFieldNamesFromBody(body, localFieldNames);

        URI uri = _httpHelper.buildUri(_indexingHandler.BaseUrl, _indexingHandler.Port, _indexingHandler.PathPrefix+IndexingHandler.POST_PARTY_SEARCH_PATH, null);

        headersToSend.add("Content-Type", "application/json");

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
    @Path("/catalogue/{standard}/{uuid}/local")
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

    /***********************************   /cataloguelines   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cataloguelines")
    public Response getCatalogueLinesByHjids(@QueryParam("ids") List<String> hjid,
                                             @QueryParam("limit") Integer limit,
                                             @QueryParam("offset") Integer offset,
                                             @QueryParam("sortOption") String sortOption,
                                             @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get catalog line by hjid (catalog service call)");
        HashMap<String, List<String>> queryParams = new HashMap<String, List<String>>();
        if (hjid != null) {
            queryParams.put("ids", hjid);
        }
        if (limit != null) {
            queryParams.put("limit", Arrays.asList(limit.toString()));
        }
        if (offset != null) {
            queryParams.put("offset", Arrays.asList(offset.toString()));
        }
        if (sortOption != null) {
            queryParams.put("sortOption", Arrays.asList(sortOption));
        }
        return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), CatalogHandler.GET_CATALOG_LINES_BY_HJIDS_LOCAL_PATH, queryParams,true);
    }

    // a REST call that should be used between delegates.
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/cataloguelines/local")
    public Response getCatalogueLinesByHjidsLocal(@QueryParam("ids") List<String> hjid,
                                                  @QueryParam("limit") Integer limit,
                                                  @QueryParam("offset") Integer offset,
                                                  @QueryParam("sortOption") String sortOption,
                                                  @Context HttpHeaders headers) throws JsonParseException, JsonMappingException, IOException {
        if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if (hjid != null) {
            queryParams.put("ids", getStringQueryParam(hjid));
        }
        if (limit != null) {
            queryParams.put("limit", limit.toString());
        }
        if (offset != null) {
            queryParams.put("offset", offset.toString());
        }
        if (sortOption != null) {
            queryParams.put("sortOption", sortOption);
        }
        URI uri = _httpHelper.buildUriWithStringParams(_catalogHandler.BaseUrl, _catalogHandler.Port, _catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_LINES_BY_HJIDS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LINES_BY_HJIDS_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   /cataloguelines - END   ***********************************/

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
        if (lineIds != null && !lineIds.isEmpty()) {
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
        if (lineIds != null && !lineIds.isEmpty()) {
            queryParams.put("lineIds", lineIds);
        }
        URI catalogServiceUri = _httpHelper.buildUri(_catalogHandler.BaseUrl, _catalogHandler.Port, String.format(_catalogHandler.PathPrefix+CatalogHandler.GET_CATALOG_LINES_PATH, catalogueUuid), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(CatalogHandler.GET_CATALOG_LINES_LOCAL_PATH, catalogServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   catalog-service/catalogue/{catalogueUuid}/cataloguelines - END   ************************************/

    /****************************************   catalog-service/catalogue/{catalogueUuid}/cataloguelines   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/catalogue/cataloguelines")
    public Response getCatalogueLines(@QueryParam("catalogueUuids") List<String> catalogueUuids, @Context HttpHeaders headers, @QueryParam("lineIds") List<String> lineIds,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get catalog lines (catalog service call)");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if (lineIds != null) {
            queryParams.put("lineIds", getStringQueryParam(lineIds));
        }
        if (catalogueUuids != null) {
            queryParams.put("catalogueUuids", getStringQueryParam(catalogueUuids));
        }

        ServiceEndpoint nimbleInfo = _eurekaHandler.getEndpointByAppName(delegateId);
        URI targetUri = _httpHelper.buildUriWithStringParams(nimbleInfo.getHostName(), nimbleInfo.getPort(), CatalogHandler.GET_MULTIPLE_CATALOG_LINES_LOCAL_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());

        return _httpHelper.sendGetRequest(targetUri, headersToSend);
    }

    // a REST call that should be used between delegates.
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/catalogue/cataloguelines/local")
    public Response getCatalogueLinesLocal(@QueryParam("catalogueUuids") List<String> catalogueUuids, @Context HttpHeaders headers, @QueryParam("lineIds") List<String> lineIds) throws JsonParseException, JsonMappingException, IOException {
        if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if (lineIds != null) {
            queryParams.put("lineIds", getStringQueryParam(lineIds));
        }
        if (catalogueUuids != null) {
            queryParams.put("catalogueUuids", getStringQueryParam(catalogueUuids));
        }
        URI catalogServiceUri = _httpHelper.buildUriWithStringParams(_catalogHandler.BaseUrl, _catalogHandler.Port, _catalogHandler.PathPrefix+CatalogHandler.GET_MULTIPLE_CATALOG_LINES_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(CatalogHandler.GET_MULTIPLE_CATALOG_LINES_LOCAL_PATH, catalogServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   catalog-service/catalogue/{catalogueUuid}/cataloguelines - END   ************************************/

    /***********************************   catalog-service/binary-contents   ***********************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/binary-contents")
    public Response getBinaryContents(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) throws IOException {
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

//        // TODO change and send to all delegates
//        ServiceEndpoint nimbleInfo = _eurekaHandler.getEndpointByAppName(headers.getHeaderString("nimbleInstanceName"));
//        URI targetUri = _httpHelper.buildUri(nimbleInfo.getHostName(), nimbleInfo.getPort(), CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, queryParams);
//
//        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
//        //TODO change to real token _identityFederationHandler.getAccessToken()
//        headersToSend.add(HttpHeaders.AUTHORIZATION, "delegate access token in the federation identity service");
//
//        // TODO send to all delegates and aggregate results
//        return _httpHelper.sendGetRequest(targetUri, headersToSend);

        return catalogServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, queryParams,true);
    }

    // a REST call that should be used between delegates.
    // the origin delegate sends a request and the target delegate will perform the query locally.
    @GET
    @Path("/binary-contents/local")
    public Response getBinaryContentsLocal(@Context HttpHeaders headers, @QueryParam("uris") List<String> uris) throws JsonParseException, JsonMappingException, IOException {
        if (_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION)) == false) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("uris", getStringQueryParam(uris));
        URI uri = _httpHelper.buildUriWithStringParams(_catalogHandler.BaseUrl, _catalogHandler.Port, _catalogHandler.PathPrefix+CatalogHandler.GET_BINARY_CONTENTS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(CatalogHandler.GET_BINARY_CONTENTS_LOCAL_PATH, uri.toString(), headersToSend, _frontendServiceUrl);
    }
    /***********************************   catalog-service/binary-contents - END   ***********************************/

    /***********************************   catalog-service - helper function   ***********************************/
    private Response catalogServiceCallWrapper(String userAccessToken, String pathToSendRequest, HashMap<String, List<String>> queryParams) throws JsonParseException, JsonMappingException, IOException {
        return catalogServiceCallWrapper(userAccessToken,pathToSendRequest,queryParams,false);
    }
    private Response catalogServiceCallWrapper(String userAccessToken, String pathToSendRequest, HashMap<String, List<String>> queryParams,Boolean mergeResponses) throws JsonParseException, JsonMappingException, IOException {
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
        if(mergeResponses){
            return Response.status(Response.Status.OK)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(CatalogHandler.mergeListResults(delegatesResponse))
                    .build();
        }
        return _catalogHandler.buildResponseFromSingleDelegate(delegatesResponse);
    }
    /***********************************   catalog-service - helper function - END   ***********************************/

    /************************************************   CATALOG SERVICE - END   ************************************************/

    /***************************************************   BUSINESS PROCESS SERVICE   ***************************************************/

    /****************************************   /document/json/{documentID}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/document/json/{documentID}")
    public Response getDocumentJsonContent(@Context HttpHeaders headers,@PathParam("documentID") String documentID,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document json content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_DOCUMENT_JSON_CONTENT_LOCAL_PATH, documentID), null,null,delegateId);
    }

    @GET
    @Path("/document/json/{documentID}/local")
    public Response getDocumentJsonContentLocal(@Context HttpHeaders headers, @PathParam("documentID") String documentID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_DOCUMENT_JSON_CONTENT_PATH, documentID), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_DOCUMENT_JSON_CONTENT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /document/json/{documentID} - END   ************************************/

    /****************************************   /document/xml/{documentID}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/document/xml/{documentID}")
    public Response getDocumentXmlContent(@Context HttpHeaders headers,@PathParam("documentID") String documentID,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_DOCUMENT_XML_CONTENT_LOCAL_PATH, documentID), null,null,delegateId);
    }

    @GET
    @Path("/document/xml/{documentID}/local")
    public Response getDocumentXmlContentLocal(@Context HttpHeaders headers, @PathParam("documentID") String documentID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_DOCUMENT_XML_CONTENT_PATH, documentID), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_DOCUMENT_XML_CONTENT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /document/xml/{documentID} - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}/archive")
    public Response archiveCollaborationGroup(@Context HttpHeaders headers,@PathParam("id") String id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.ARCHIVE_COLLABORATION_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}/archive/local")
    public Response archiveCollaborationGroupLocal(@Context HttpHeaders headers, @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.ARCHIVE_COLLABORATION_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.ARCHIVE_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}")
    public Response deleteCollaborationGroup(@Context HttpHeaders headers,@PathParam("id") String id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("DELETE",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.DELETE_COLLABORATION_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @DELETE
    @Path("/collaboration-groups/{id}/local")
    public Response deleteCollaborationGroupLocal(@Context HttpHeaders headers, @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.DELETE_COLLABORATION_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardDeleteRequestWithStringBody(BusinessProcessHandler.DELETE_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}")
    public Response getCollaborationGroup(@Context HttpHeaders headers,@PathParam("id") String id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_COLLABORATION_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @GET
    @Path("/collaboration-groups/{id}/local")
    public Response getCollaborationGroupLocal(@Context HttpHeaders headers, @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_COLLABORATION_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}/restore")
    public Response restoreCollaborationGroup(@Context HttpHeaders headers,@PathParam("id") String id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.RESTORE_COLLABORATION_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}/restore/local")
    public Response restoreCollaborationGroupLocal(@Context HttpHeaders headers, @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.RESTORE_COLLABORATION_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.RESTORE_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/processInstance/{processInstanceId}/cancel")
    public Response cancelProcessInstance(@Context HttpHeaders headers,@PathParam("processInstanceId") String processInstanceId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.CANCEL_PROCESS_INSTANCE_LOCAL_PATH, processInstanceId), null,null,delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/processInstance/{processInstanceId}/cancel/local")
    public Response cancelProcessInstanceLocal(@Context HttpHeaders headers, @PathParam("processInstanceId") String processInstanceId) throws JsonParseException, JsonMappingException, IOException {
//        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
//            return Response.status(Response.Status.UNAUTHORIZED).build();
//        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.CANCEL_PROCESS_INSTANCE_PATH, processInstanceId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.CANCEL_PROCESS_INSTANCE_LOCAL_PATH, businessProcessServiceUri.toString(), null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/processInstance/{processInstanceId}/isRated")
    public Response isRated(@Context HttpHeaders headers,@PathParam("processInstanceId") String processInstanceId,@QueryParam("partyId") String partyId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.IS_RATED_LOCAL_PATH, processInstanceId), queryParams,null,headers.getHeaderString("federationId"),delegateId);
    }

    @GET
    @Path("/processInstance/{processInstanceId}/isRated/local")
    public Response isRatedLocal(@Context HttpHeaders headers, @PathParam("processInstanceId") String processInstanceId,@QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.IS_RATED_PATH, processInstanceId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.IS_RATED_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/ratingsAndReviews")
    public Response createRatingAndReview(@Context HttpHeaders headers,
                                          @QueryParam("ratings") String ratingsString,
                                          @QueryParam("reviews") String reviewsString,
                                          @QueryParam("partyId") String partyId,
                                          @QueryParam("processInstanceID") String processInstanceID,
                                          @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("ratings", URLEncoder.encode(ratingsString,"UTF-8"));
        queryParams.put("reviews", URLEncoder.encode(reviewsString,"UTF-8"));
        queryParams.put("partyId", partyId);
        queryParams.put("processInstanceID", processInstanceID);
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.CREATE_RATINGS_AND_REVIEWS_LOCAL_PATH, queryParams,null,headers.getHeaderString("federationId"),delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/ratingsAndReviews/local")
    public Response createRatingAndReviewLocal(@Context HttpHeaders headers,
                                               @QueryParam("ratings") String ratingsString,
                                               @QueryParam("reviews") String reviewsString,
                                               @QueryParam("partyId") String partyId,
                                               @QueryParam("processInstanceID") String processInstanceID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("ratings", URLEncoder.encode(ratingsString,"UTF-8"));
        queryParams.put("reviews", URLEncoder.encode(reviewsString,"UTF-8"));
        queryParams.put("partyId", partyId);
        queryParams.put("processInstanceID", processInstanceID);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.CREATE_RATINGS_AND_REVIEWS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.CREATE_RATINGS_AND_REVIEWS_LOCAL_PATH, businessProcessServiceUri.toString(), null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/continue")
    public Response continueProcessInstance(@Context HttpHeaders headers,
                                            String body,
                                            @QueryParam("gid") String gid,
                                            @QueryParam("collaborationGID") String collaborationGID,
                                            @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("gid", gid);
        queryParams.put("collaborationGID", collaborationGID);
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.CONTINUE_PROCESS_INSTANCE_LOCAL_PATH, queryParams,body,headers.getHeaderString("initiatorFederationId"),headers.getHeaderString("responderFederationIdHeader"),delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/continue/local")
    public Response continueProcessInstanceLocal(@Context HttpHeaders headers,
                                                 String body,
                                                 @QueryParam("gid") String gid,
                                                 @QueryParam("collaborationGID") String collaborationGID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("gid", gid);
        queryParams.put("collaborationGID", collaborationGID);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.CONTINUE_PROCESS_INSTANCE_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("initiatorFederationId", headers.getRequestHeader("initiatorFederationId").get(0));
        headersToSend.add("responderFederationId", headers.getRequestHeader("responderFederationId").get(0));
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.CONTINUE_PROCESS_INSTANCE_LOCAL_PATH, businessProcessServiceUri.toString(), body,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/process-document")
    public Response startProcessWithDocument(@Context HttpHeaders headers,
                                             String body,
                                             @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        Response response = businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.START_PROCESS_WITH_DOCUMENT_LOCAL_PATH, null,body,delegateId);
        return response;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/process-document/local")
    public Response startProcessWithDocumentLocal(@Context HttpHeaders headers,
                                                  String body) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.START_PROCESS_WITH_DOCUMENT_PATH, null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.START_PROCESS_WITH_DOCUMENT_LOCAL_PATH, businessProcessServiceUri.toString(), body,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/start")
    public Response startProcessInstance(@Context HttpHeaders headers,
                                         String body,
                                         @QueryParam("gid") String gid,
                                         @QueryParam("precedingGid") String precedingGid,
                                         @QueryParam("collaborationGID") String collaborationGID,
                                         @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("gid", gid);
        queryParams.put("collaborationGID", collaborationGID);
        queryParams.put("precedingGid", precedingGid);
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.START_PROCESS_WITH_DOCUMENT_LOCAL_PATH, queryParams,body,headers.getHeaderString("initiatorFederationId"),headers.getHeaderString("responderFederationIdHeader"),delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/start/local")
    public Response startProcessInstanceLocal(@Context HttpHeaders headers,
                                              String body,
                                              @QueryParam("gid") String gid,
                                              @QueryParam("precedingGid") String precedingGid,
                                              @QueryParam("collaborationGID") String collaborationGID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("gid", gid);
        queryParams.put("collaborationGID", collaborationGID);
        queryParams.put("precedingGid", precedingGid);

        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.START_PROCESS_INSTANCE_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("initiatorFederationId", headers.getRequestHeader("initiatorFederationId").get(0));
        headersToSend.add("responderFederationId", headers.getRequestHeader("responderFederationId").get(0));
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.START_PROCESS_INSTANCE_LOCAL_PATH, businessProcessServiceUri.toString(), body,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/processInstance/{processInstanceId}/collaboration-group")
    public Response getAssociatedCollaborationGroup(@Context HttpHeaders headers,
                                                    @PathParam("processInstanceId") String processInstanceId,
                                                    @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_ASSOCIATED_COLLABORATION_GROUP_LOCAL_PATH, processInstanceId), null,null,delegateId);
    }

    @GET
    @Path("/processInstance/{processInstanceId}/collaboration-group/local")
    public Response getAssociatedCollaborationGroupLocal(@Context HttpHeaders headers,
                                                         @PathParam("processInstanceId") String processInstanceId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_ASSOCIATED_COLLABORATION_GROUP_PATH, processInstanceId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_ASSOCIATED_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}")
    public Response deleteProcessInstanceGroup(@Context HttpHeaders headers,
                                               @PathParam("id") String id,
                                               @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("DELETE",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.DELETE_PROCESS_INSTANCE_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @DELETE
    @Path("/process-instance-groups/{id}/local")
    public Response deleteProcessInstanceGroupLocal(@Context HttpHeaders headers,
                                                    @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.DELETE_PROCESS_INSTANCE_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardDeleteRequestWithStringBody(BusinessProcessHandler.DELETE_PROCESS_INSTANCE_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}")
    public Response getProcessInstanceGroup(@Context HttpHeaders headers,
                                            @PathParam("id") String id,
                                            @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @GET
    @Path("/process-instance-groups/{id}")
    public Response getProcessInstanceGroupLocal(@Context HttpHeaders headers,
                                                 @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}/archive   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}/cancel")
    public Response cancelCollaboration(@Context HttpHeaders headers,
                                        @PathParam("id") String id,
                                        String cancellationReason,
                                        @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.CANCEL_COLLABORATION_LOCAL_PATH,id), null,cancellationReason,delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}/cancel/local")
    public Response cancelCollaborationLocal(@Context HttpHeaders headers,
                                        @PathParam("id") String id,
                                        String cancellationReason) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.CANCEL_COLLABORATION_PATH,id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.CANCEL_COLLABORATION_LOCAL_PATH, businessProcessServiceUri.toString(), cancellationReason,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id}/archive - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}/finished")
    public Response checkCollaborationFinished(@Context HttpHeaders headers,
                                               @PathParam("id") String id,
                                               @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated check collaboration finished");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.CHECK_COLLABORATION_FINISHED_LOCAL_PATH, id), null,null,delegateId);
    }

    @GET
    @Path("/process-instance-groups/{id}/finished/local")
    public Response checkCollaborationFinishedLocal(@Context HttpHeaders headers,
                                                    @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.CHECK_COLLABORATION_FINISHED_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.CHECK_COLLABORATION_FINISHED_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}/process-instances")
    public Response getProcessInstancesIncludedInTheGroup(@Context HttpHeaders headers,
                                                          @PathParam("id") String id,
                                                          @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_PROCESS_INSTANCES_INCLUDED_IN_THE_GROUP_LOCAL_PATH, id), null,null,delegateId);
    }

    @GET
    @Path("/process-instance-groups/{id}/process-instances/local")
    public Response getProcessInstancesIncludedInTheGroupLocal(@Context HttpHeaders headers,
                                                               @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_INSTANCES_INCLUDED_IN_THE_GROUP_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_INSTANCES_INCLUDED_IN_THE_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/all-finished")
    public Response checkAllCollaborationsFinished(@Context HttpHeaders headers,
                                                   @QueryParam("partyId") String partyId,
                                                   @QueryParam("collaborationRole") @DefaultValue("SELLER") String collaborationRole) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("collaborationRole", collaborationRole);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.CHECK_ALL_COLLABORATIONS_FINISHED_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.BooleanResults);
    }

    @GET
    @Path("/collaboration-groups/all-finished/local")
    public Response checkAllCollaborationsFinishedLocal(@Context HttpHeaders headers,
                                                        @QueryParam("partyId") String partyId,
                                                        @QueryParam("collaborationRole") @DefaultValue("SELLER") String collaborationRole) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("collaborationRole", collaborationRole);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.CHECK_ALL_COLLABORATIONS_FINISHED_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.CHECK_ALL_COLLABORATIONS_FINISHED_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/total-number/business-process/action-required")
    public Response getActionRequiredProcessCount(@Context HttpHeaders headers,
                                                  @QueryParam("partyId") String partyId,
                                                  @QueryParam("archived") Boolean archived,
                                                  @QueryParam("role") String role) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("archived", archived.toString());
        queryParams.put("role", role);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_ACTION_REQUIRED_PROCESS_COUNT_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.DoubleResults);
    }

    @GET
    @Path("/statistics/total-number/business-process/action-required/local")
    public Response getActionRequiredProcessCountLocal(@Context HttpHeaders headers,
                                                       @QueryParam("partyId") String partyId,
                                                       @QueryParam("archived") Boolean archived,
                                                       @QueryParam("role") String role) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("archived", archived.toString());
        queryParams.put("role", role);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_ACTION_REQUIRED_PROCESS_COUNT_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_ACTION_REQUIRED_PROCESS_COUNT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/total-number/business-process")
    public Response getProcessCount(@Context HttpHeaders headers,
                                    @QueryParam("businessProcessType") String businessProcessType,
                                    @QueryParam("startDateStr") String startDateStr,
                                    @QueryParam("endDateStr") String endDateStr,
                                    @QueryParam("partyId") String partyId,
                                    @QueryParam("role") String role,
                                    @QueryParam("status") String status) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("businessProcessType", businessProcessType);
        queryParams.put("startDateStr", startDateStr);
        queryParams.put("endDateStr", endDateStr);
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        queryParams.put("status", status);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_PROCESS_COUNT_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.DoubleResults);
    }

    @GET
    @Path("/statistics/total-number/business-process/local")
    public Response getProcessCountLocal(@Context HttpHeaders headers,
                                         @QueryParam("businessProcessType") String businessProcessType,
                                         @QueryParam("startDateStr") String startDateStr,
                                         @QueryParam("endDateStr") String endDateStr,
                                         @QueryParam("partyId") String partyId,
                                         @QueryParam("role") String role,
                                         @QueryParam("status") String status) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("businessProcessType", businessProcessType);
        queryParams.put("startDateStr", startDateStr);
        queryParams.put("endDateStr", endDateStr);
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        queryParams.put("status", status);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_COUNT_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_COUNT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/trading-volume")
    public Response getTradingVolume(@Context HttpHeaders headers,
                                     @QueryParam("startDateStr") String startDateStr,
                                     @QueryParam("endDateStr") String endDateStr,
                                     @QueryParam("partyId") String partyId,
                                     @QueryParam("role") String role,
                                     @QueryParam("status") String status) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("startDateStr", startDateStr);
        queryParams.put("endDateStr", endDateStr);
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        queryParams.put("status", status);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_TRADING_VOLUME_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.DoubleResults);
    }

    @GET
    @Path("/statistics/trading-volume/local")
    public Response getTradingVolumeLocal(@Context HttpHeaders headers,
                                          @QueryParam("startDateStr") String startDateStr,
                                          @QueryParam("endDateStr") String endDateStr,
                                          @QueryParam("partyId") String partyId,
                                          @QueryParam("role") String role,
                                          @QueryParam("status") String status) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("startDateStr", startDateStr);
        queryParams.put("endDateStr", endDateStr);
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        queryParams.put("status", status);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_TRADING_VOLUME_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_TRADING_VOLUME_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/response-time")
    public Response getAverageResponseTime(@Context HttpHeaders headers,
                                           @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.DoubleResults);
    }

    @GET
    @Path("/statistics/response-time/local")
    public Response getAverageResponseTimeLocal(@Context HttpHeaders headers,
                                                @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/collaboration-time")
    public Response getAverageCollaborationTime(@Context HttpHeaders headers,
                                                @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_AVERAGE_COLLABORATION_TIME_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.DoubleResults);
    }

    @GET
    @Path("/statistics/collaboration-time/local")
    public Response getAverageCollaborationTimeLocal(@Context HttpHeaders headers,
                                                     @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_AVERAGE_COLLABORATION_TIME_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_AVERAGE_COLLABORATION_TIME_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ratingsSummary")
    public Response getRatingsSummary(@Context HttpHeaders headers,
                                      @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_RATING_SUMMARY_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.RatingSummaries);
    }

    @GET
    @Path("/ratingsSummary/local")
    public Response getRatingsSummaryLocal(@Context HttpHeaders headers,
                                           @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_RATING_SUMMARY_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_RATING_SUMMARY_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/federated")
    public Response getFederatedCollaborationGroup(@Context HttpHeaders headers,
                                                   @QueryParam("id") List<String> groupId,
                                                   @QueryParam("federationId") List<String> federationId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("id", getStringQueryParam(groupId));
        queryParams.put("federationId", getStringQueryParam(federationId));
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_FEDERATED_COLLABORATION_GROUP_LOCAL_PATH, queryParams, MergeOption.CollaborationGroups);
    }

    @GET
    @Path("/collaboration-groups/federated/local")
    public Response getFederatedCollaborationGroupLocal(@Context HttpHeaders headers,
                                                        @QueryParam("id") List<String> groupId,
                                                        @QueryParam("federationId") List<String> federationId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("id", getStringQueryParam(groupId));
        queryParams.put("federationId", getStringQueryParam(federationId));
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_FEDERATED_COLLABORATION_GROUP_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_FEDERATED_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups")
    public Response getCollaborationGroups(@Context HttpHeaders headers,
                                           @QueryParam("partyId") String partyId,
                                           @QueryParam("relatedProducts") List<String> relatedProducts,
                                           @QueryParam("relatedProductCategories") List<String> relatedProductCategories,
                                           @QueryParam("tradingPartnerIDs") List<String> tradingPartnerIDs,
                                           @QueryParam("offset") @DefaultValue("0") Integer offset,
                                           @QueryParam("limit") @DefaultValue("10") Integer limit,
                                           @QueryParam("archived") @DefaultValue("false") Boolean archived,
                                           @QueryParam("status") List<String> status,
                                           @QueryParam("collaborationRole") String collaborationRole,
                                           @QueryParam("isProject") @DefaultValue("false") Boolean isProject,
                                           @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get collaboration groups");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("relatedProducts", getStringQueryParam(relatedProducts));
        queryParams.put("tradingPartnerIDs", getStringQueryParam(tradingPartnerIDs));
        queryParams.put("relatedProductCategories", getStringQueryParam(relatedProductCategories));
        queryParams.put("offset", offset.toString());
        queryParams.put("limit", limit.toString());
        queryParams.put("archived",archived.toString());
        queryParams.put("status", getStringQueryParam(status));
        queryParams.put("collaborationRole", collaborationRole);
        queryParams.put("isProject",isProject.toString());
        Response response = businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_COLLABORATION_GROUPS_LOCAL_PATH, queryParams,null,headers.getHeaderString("federationId"),delegateId);
        if(response.getStatus() == 200){
            String body = response.getEntity().toString();
            List<String> federationIds = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            JsonParser parser = new JsonParser();
            JsonObject jsonObject = parser.parse(body).getAsJsonObject();

            JsonArray collaborationGroups = jsonObject.get("collaborationGroups").getAsJsonArray();
            for (JsonElement collaborationGroup : collaborationGroups) {
                ids.add(collaborationGroup.getAsJsonObject().get("id").getAsString());
                federationIds.add(collaborationGroup.getAsJsonObject().get("federationId").getAsString());
            }
            // get federation collaborations
            Response federatedCollaborationsResponse = getFederatedCollaborationGroup(headers,ids,federationIds);
            String delegateResponse = BusinessProcessHandler.mergeCollaborationGroupAndFederatedCollaborations(body,federatedCollaborationsResponse.getEntity().toString());
            logger.info("Get collaboration group delegate response:{}",delegateResponse);
            return Response.status(Response.Status.OK)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(delegateResponse)
                    .build();
        }
        return response;
    }

    @GET
    @Path("/collaboration-groups/local")
    public Response getCollaborationGroupsLocal(@Context HttpHeaders headers,
                                                @QueryParam("partyId") String partyId,
                                                @QueryParam("relatedProducts") List<String> relatedProducts,
                                                @QueryParam("relatedProductCategories") List<String> relatedProductCategories,
                                                @QueryParam("tradingPartnerIDs") List<String> tradingPartnerIDs,
                                                @QueryParam("offset") @DefaultValue("0") Integer offset,
                                                @QueryParam("limit") @DefaultValue("10") Integer limit,
                                                @QueryParam("archived") @DefaultValue("false") Boolean archived,
                                                @QueryParam("status") List<String> status,
                                                @QueryParam("collaborationRole") String collaborationRole,
                                                @QueryParam("isProject") @DefaultValue("false") Boolean isProject) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("relatedProducts", getStringQueryParam(relatedProducts));
        queryParams.put("tradingPartnerIDs", getStringQueryParam(tradingPartnerIDs));
        queryParams.put("relatedProductCategories", getStringQueryParam(relatedProductCategories));
        queryParams.put("offset", offset.toString());
        queryParams.put("limit", limit.toString());
        queryParams.put("archived",archived.toString());
        queryParams.put("status", getStringQueryParam(status));
        queryParams.put("collaborationRole", collaborationRole);
        queryParams.put("isProject",isProject.toString());
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_COLLABORATION_GROUPS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_COLLABORATION_GROUPS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/filters")
    public Response getProcessInstanceGroupFilters(@Context HttpHeaders headers,
                                                   @QueryParam("partyId") String partyId,
                                                   @QueryParam("relatedProducts") List<String> relatedProducts,
                                                   @QueryParam("relatedProductCategories") List<String> relatedProductCategories,
                                                   @QueryParam("tradingPartnerIDs") List<String> tradingPartnerIDs,
                                                   @QueryParam("archived") @DefaultValue("false") Boolean archived,
                                                   @QueryParam("status") List<String> status,
                                                   @QueryParam("collaborationRole") String collaborationRole,
                                                   @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("relatedProducts", getStringQueryParam(relatedProducts));
        queryParams.put("tradingPartnerIDs", getStringQueryParam(tradingPartnerIDs));
        queryParams.put("relatedProductCategories", getStringQueryParam(relatedProductCategories));
        queryParams.put("archived",archived.toString());
        queryParams.put("status", getStringQueryParam(status));
        queryParams.put("collaborationRole", collaborationRole);
        Response response = businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_FILTERS_LOCAL_PATH, queryParams,null,headers.getHeaderString("federationId"), delegateId);
        if(response.getStatus() == 200){
            String body = response.getEntity().toString();

            JsonParser parser = new JsonParser();
            JsonObject jsonObject = parser.parse(body).getAsJsonObject();

            JsonArray instanceNames = new JsonArray();
            for (ServiceEndpoint serviceEndpoint : _eurekaHandler.getEndpointsFromEureka()) {
                instanceNames.add(serviceEndpoint.getAppName());
            }
            jsonObject.add("instanceNames",instanceNames);
            return Response.status(Response.Status.OK)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(jsonObject.toString())
                    .build();
        }
        return response;
    }

    @GET
    @Path("/process-instance-groups/filters/local")
    public Response getProcessInstanceGroupFiltersLocal(@Context HttpHeaders headers,
                                                        @QueryParam("partyId") String partyId,
                                                        @QueryParam("relatedProducts") List<String> relatedProducts,
                                                        @QueryParam("relatedProductCategories") List<String> relatedProductCategories,
                                                        @QueryParam("tradingPartnerIDs") List<String> tradingPartnerIDs,
                                                        @QueryParam("archived") @DefaultValue("false") Boolean archived,
                                                        @QueryParam("status") List<String> status,
                                                        @QueryParam("collaborationRole") String collaborationRole) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("relatedProducts", getStringQueryParam(relatedProducts));
        queryParams.put("tradingPartnerIDs", getStringQueryParam(tradingPartnerIDs));
        queryParams.put("relatedProductCategories", getStringQueryParam(relatedProductCategories));
        queryParams.put("archived",archived.toString());
        queryParams.put("status", getStringQueryParam(status));
        queryParams.put("collaborationRole", collaborationRole);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_FILTERS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_INSTANCE_GROUP_FILTERS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/processInstance/{processInstanceId}/details")
    public Response getDashboardProcessInstanceDetails(@Context HttpHeaders headers,
                                                       @PathParam("processInstanceId") String processInstanceId,
                                                       @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get dashboard process instance details");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_DASHBOARD_PROCESS_INSTANCE_DETAILS_LOCAL_PATH, processInstanceId), null,null,delegateId);
    }

    @GET
    @Path("/processInstance/{processInstanceId}/details/local")
    public Response getDashboardProcessInstanceDetailsLocal(@Context HttpHeaders headers,
                                                            @PathParam("processInstanceId") String processInstanceId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_DASHBOARD_PROCESS_INSTANCE_DETAILS_PATH, processInstanceId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_DASHBOARD_PROCESS_INSTANCE_DETAILS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/


    /****************************************   /collaboration-groups/{id}   ****************************************/
    @PATCH
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/{id}")
    public Response updateCollaborationGroupName(@Context HttpHeaders headers,
                                                 @PathParam("id") String id,
                                                 @QueryParam("groupName") String groupName,
                                                 @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update collaboration group name");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("groupName", groupName);
        return businessProcessServiceCallWrapper("PATCH",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.UPDATE_COLLABORATION_GROUP_NAME_LOCAL_PATH, id), queryParams,null,delegateId);
    }

    @PATCH
    @Path("/collaboration-groups/{id}/local")
    public Response updateCollaborationGroupNameLocal(@Context HttpHeaders headers,
                                                      @PathParam("id") String id,
                                                      @QueryParam("groupName") String groupName) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("groupName", groupName);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.UPDATE_COLLABORATION_GROUP_NAME_PATH, id), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPatchRequest(BusinessProcessHandler.UPDATE_COLLABORATION_GROUP_NAME_LOCAL_PATH, businessProcessServiceUri.toString(),null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @PATCH
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/document/{documentID}")
    public Response updateDocument(@Context HttpHeaders headers,
                                   String content,
                                   @PathParam("documentID") String documentID,
                                   @QueryParam("documentType") String documentType,
                                   @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("documentType", documentType);
        return businessProcessServiceCallWrapper("PATCH",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.UPDATE_DOCUMENT_LOCAL_PATH, documentID), queryParams,content,delegateId);
    }

    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/document/{documentID}/local")
    public Response updateDocumentLocal(@Context HttpHeaders headers,
                                        String content,
                                        @PathParam("documentID") String documentID,
                                        @QueryParam("documentType") String documentType) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("documentType", documentType);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.UPDATE_DOCUMENT_PATH, documentID), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPatchRequest(BusinessProcessHandler.UPDATE_COLLABORATION_GROUP_NAME_LOCAL_PATH, businessProcessServiceUri.toString(), content,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/


    /****************************************   /collaboration-groups/{id}   ****************************************/
    @PATCH
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/processInstance")
    public Response updateProcessInstance(@Context HttpHeaders headers,
                                          String content,
                                          @QueryParam("processInstanceID") String processInstanceID,
                                          @QueryParam("creatorUserID") String creatorUserID,
                                          @QueryParam("documentType") String documentType,
                                          @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("documentType", documentType);
        queryParams.put("creatorUserID", creatorUserID);
        queryParams.put("processInstanceID", processInstanceID);
        return businessProcessServiceCallWrapper("PATCH",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.UPDATE_PROCESS_INSTANCE_LOCAL_PATH, queryParams,content,delegateId);
    }

    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/processInstance/local")
    public Response updateProcessInstanceLocal(@Context HttpHeaders headers,
                                               String content,
                                               @QueryParam("processInstanceID") String processInstanceID,
                                               @QueryParam("creatorUserID") String creatorUserID,
                                               @QueryParam("documentType") String documentType) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("documentType", documentType);
        queryParams.put("creatorUserID", creatorUserID);
        queryParams.put("processInstanceID", processInstanceID);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.UPDATE_PROCESS_INSTANCE_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPatchRequest(BusinessProcessHandler.UPDATE_PROCESS_INSTANCE_LOCAL_PATH, businessProcessServiceUri.toString(), content,headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/order-document")
    public Response getOrderDocument(@Context HttpHeaders headers,
                                     @QueryParam("processInstanceId") String processInstanceId,
                                     @QueryParam("orderResponseId") String orderResponseId,
                                     @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("processInstanceId", processInstanceId);
        if(orderResponseId != null){
            queryParams.put("orderResponseId", orderResponseId);
        }
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_ORDER_DOCUMENT_LOCAL_PATH, queryParams,null,delegateId);
    }

    @GET
    @Path("/process-instance-groups/order-document/local")
    public Response getOrderDocumentLocal(@Context HttpHeaders headers,
                                          @QueryParam("processInstanceId") String processInstanceId,
                                          @QueryParam("orderResponseId") String orderResponseId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("processInstanceId", processInstanceId);
        if(orderResponseId != null){
            queryParams.put("orderResponseId", orderResponseId);
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_ORDER_DOCUMENT_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_ORDER_DOCUMENT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ratingsAndReviews")
    public Response listAllIndividualRatingsAndReviews(@Context HttpHeaders headers,
                                                       @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.LIST_ALL_INDIVIDUAL_RATINGS_AND_REVIEWS_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.IndividualRatingsAndReviews);
    }

    @GET
    @Path("/ratingsAndReviews/local")
    public Response listAllIndividualRatingsAndReviewsLocal(@Context HttpHeaders headers,
                                                            @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.LIST_ALL_INDIVIDUAL_RATINGS_AND_REVIEWS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.LIST_ALL_INDIVIDUAL_RATINGS_AND_REVIEWS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/


    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contracts")
    public Response constructContractForProcessInstances(@Context HttpHeaders headers,
                                                         @QueryParam("processInstanceId") String processInstanceId,
                                                         @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("processInstanceId", processInstanceId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.CONSTRUCT_CONTRACT_FOR_PROCESS_INSTANCE_LOCAL_PATH, queryParams,null,delegateId);
    }

    @GET
    @Path("/contracts/local")
    public Response constructContractForProcessInstancesLocal(@Context HttpHeaders headers,
                                                              @QueryParam("processInstanceId") String processInstanceId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("processInstanceId", processInstanceId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.CONSTRUCT_CONTRACT_FOR_PROCESS_INSTANCE_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.CONSTRUCT_CONTRACT_FOR_PROCESS_INSTANCE_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contracts/{contractId}/clauses")
    public Response getClausesOfContract(@Context HttpHeaders headers,
                                         @PathParam("contractId") String contractId,
                                         @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_CLAUSES_OF_CONTRACT_LOCAL_PATH,contractId), queryParams,null,delegateId);
    }

    @GET
    @Path("/contracts/{contractId}/clauses/local")
    public Response getClausesOfContractLocal(@Context HttpHeaders headers,
                                              @PathParam("contractId") String contractId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.GET_CLAUSES_OF_CONTRACT_PATH,contractId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_CLAUSES_OF_CONTRACT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/documents/{documentId}/clauses")
    public Response getClauseDetails(@Context HttpHeaders headers,
                                     @PathParam("documentId") String documentId,
                                     @QueryParam("clauseType") String clauseType,
                                     @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("clauseType",clauseType);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_CLAUSE_DETAILS_LOCAL_PATH,documentId), queryParams,null,delegateId);
    }

    @GET
    @Path("/documents/{documentId}/clauses/local")
    public Response getClauseDetailsLocal(@Context HttpHeaders headers,
                                          @PathParam("documentId") String documentId,
                                          @QueryParam("clauseType") String clauseType) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("clauseType",clauseType);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.GET_CLAUSE_DETAILS_PATH,documentId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_CLAUSE_DETAILS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/documents/{documentId}/contract/clause/document")
    public Response addDocumentClauseToContract(@Context HttpHeaders headers,
                                                @PathParam("documentId") String documentId,
                                                @QueryParam("clauseDocumentId") String clauseDocumentId,
                                                @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("clauseDocumentId",clauseDocumentId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.ADD_DOCUMENT_CLAUSE_TO_CONTRACT_LOCAL_PATH,documentId), queryParams,null,delegateId);
    }

    @GET
    @Path("/documents/{documentId}/contract/clause/document/local")
    public Response addDocumentClauseToContractLocal(@Context HttpHeaders headers,
                                                     @PathParam("documentId") String documentId,
                                                     @QueryParam("clauseDocumentId") String clauseDocumentId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("clauseDocumentId",clauseDocumentId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.ADD_DOCUMENT_CLAUSE_TO_CONTRACT_PATH,documentId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.ADD_DOCUMENT_CLAUSE_TO_CONTRACT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @PATCH
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/documents/{documentId}/contract/clause/data-monitoring")
    public Response addDataMonitoringClauseToContract(@Context HttpHeaders headers,
                                                      @PathParam("documentId") String documentId,
                                                      String dataMonitoringClause,
                                                      @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        return businessProcessServiceCallWrapper("PATCH",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_LOCAL_PATH,documentId), queryParams,dataMonitoringClause,delegateId);
    }

    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/documents/{documentId}/contract/clause/data-monitoring/local")
    public Response addDataMonitoringClauseToContractLocal(@Context HttpHeaders headers,
                                                           @PathParam("documentId") String documentId,
                                                           String dataMonitoringClause) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_PATH,documentId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPatchRequest(BusinessProcessHandler.ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_LOCAL_PATH, businessProcessServiceUri.toString(), dataMonitoringClause,headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contract/digital-agreement/{id}")
    public Response getDigitalAgreementForPartiesAndProduct(@Context HttpHeaders headers,
                                                            @PathParam("id") String id,
                                                            @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_LOCAL_PATH,id), queryParams,null,delegateId);
    }

    @GET
    @Path("/contract/digital-agreement/{id}/local")
    public Response getDigitalAgreementForPartiesAndProductLocal(@Context HttpHeaders headers,
                                                                 @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_PATH,id), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.ADD_DATA_MONITORING_CLAUSE_TO_CONTRACT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contract/digital-agreement")
    public Response getDigitalAgreementForPartiesAndProduct(@Context HttpHeaders headers,
                                                            @QueryParam("buyerId") String buyerId,
                                                            @QueryParam("sellerId") String sellerId,
                                                            @QueryParam("productIds") List<String> productIds,
                                                            @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("productIds",getStringQueryParam(productIds));
        queryParams.put("sellerId",sellerId);
        queryParams.put("buyerId",buyerId);

        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_2_LOCAL_PATH, queryParams,null,headers.getHeaderString("initiatorFederationId"),headers.getHeaderString("responderFederationId"),delegateId);
    }

    @GET
    @Path("/contract/digital-agreement/local")
    public Response getDigitalAgreementForPartiesAndProductLocal(@Context HttpHeaders headers,
                                                                 @QueryParam("buyerId") String buyerId,
                                                                 @QueryParam("sellerId") String sellerId,
                                                                 @QueryParam("productIds") List<String> productIds) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("productIds",getStringQueryParam(productIds));
        queryParams.put("sellerId",sellerId);
        queryParams.put("buyerId",buyerId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_2_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("initiatorFederationId", headers.getRequestHeader("initiatorFederationId").get(0));
        headersToSend.add("responderFederationId", headers.getRequestHeader("responderFederationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_2_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contract/digital-agreement/all")
    public Response getDigitalAgreementForPartiesAndProductAll(@Context HttpHeaders headers,
                                                               @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_3_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"),MergeOption.ListResults);
    }

    @GET
    @Path("/contract/digital-agreement/all/local")
    public Response getDigitalAgreementForPartiesAndProductAllLocal(@Context HttpHeaders headers,
                                                                    @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_3_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_DIGITAL_AGREEMENT_FOR_PARTIES_AND_PRODUCT_3_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/document/{documentId}/group-id-tuple")
    public Response getGroupIdTuple(@Context HttpHeaders headers,
                                    @PathParam("documentId") String documentId,
                                    @QueryParam("partyId") String partyId,
                                    @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_GROUP_ID_TUPLE_LOCAL_PATH,documentId), queryParams,null,headers.getHeaderString("federationId"),delegateId);
    }

    @GET
    @Path("/document/{documentId}/group-id-tuple/local")
    public Response getGroupIdTupleLocal(@Context HttpHeaders headers,
                                         @PathParam("documentId") String documentId,
                                         @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.GET_GROUP_ID_TUPLE_PATH,documentId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_GROUP_ID_TUPLE_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/unmerge")
    public Response unMergeCollaborationGroup(@Context HttpHeaders headers,
                                              @QueryParam("groupId") String groupId,
                                              @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("groupId",groupId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.UNMERGE_COLLABORATION_GROUP_LOCAL_PATH, queryParams,null,delegateId);
    }

    @GET
    @Path("/collaboration-groups/unmerge/local")
    public Response unMergeCollaborationGroupLocal(@Context HttpHeaders headers,
                                                   @QueryParam("groupId") String groupId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("groupId",groupId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.UNMERGE_COLLABORATION_GROUP_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.UNMERGE_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/document/{documentId}")
    public Response addFederatedMetadataToCollaborationGroup(@Context HttpHeaders headers,
                                                             @PathParam("documentId") String documentId,
                                                             String body,
                                                             @QueryParam("partyId") String partyId,
                                                             @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.ADD_FEDERATED_METADATA_TO_COLLABORATION_GROUP_LOCAL_PATH,documentId), queryParams,body,headers.getHeaderString("federationId"),delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/document/{documentId}/local")
    public Response addFederatedMetadataToCollaborationGroupLocal(@Context HttpHeaders headers,
                                                                  @PathParam("documentId") String documentId,
                                                                  String body,
                                                                  @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+String.format(BusinessProcessHandler.ADD_FEDERATED_METADATA_TO_COLLABORATION_GROUP_PATH,documentId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.ADD_FEDERATED_METADATA_TO_COLLABORATION_GROUP_LOCAL_PATH, businessProcessServiceUri.toString(),body, headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces("application/zip")
    @Path("/processInstance/export")
    public void exportProcessInstanceData(@Context HttpHeaders headers,
                                          @QueryParam("userId") String userId,
                                          @QueryParam("direction") String direction,
                                          @QueryParam("archived") Boolean archived,
                                          @QueryParam("partyId") String partyId,
                                          @Context  HttpServletResponse response) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        logger.info("Data ServletResponse is null: {}",response == null);
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        if(direction != null){
            queryParams.put("direction",direction);
        }
        if(userId != null){
            queryParams.put("userId",userId);
        }
        if(archived != null){
            queryParams.put("archived",archived.toString());
        }
        businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.EXPORT_PROCESS_INSTANCE_DATA_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"),response);
    }

    @GET
    @Produces("application/zip")
    @Path("/processInstance/export/local")
    public void exportProcessInstanceDataLocal(@Context HttpHeaders headers,
                                               @QueryParam("userId") String userId,
                                               @QueryParam("direction") String direction,
                                               @QueryParam("archived") Boolean archived,
                                               @QueryParam("partyId") String partyId,
                                               @Context   HttpServletResponse response) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            return;
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId",partyId);
        if(direction != null){
            queryParams.put("direction",direction);
        }
        if(userId != null){
            queryParams.put("userId",userId);
        }
        if(archived != null){
            queryParams.put("archived",archived.toString());
        }
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.EXPORT_PROCESS_INSTANCE_DATA_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId",headers.getHeaderString("federationId"));
        headersToSend.add("Accept","application/zip");
        try {
            _httpHelper.forwardZipRequest(BusinessProcessHandler.EXPORT_PROCESS_INSTANCE_DATA_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl,response);
        } catch (Exception e) {
            logger.error("Failed to zip response:",e);
            response.setStatus(Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/merge")
    public Response mergeCollaborationGroups(@Context HttpHeaders headers,
                                             String body,
                                             @QueryParam("bcid") String bcid,
                                             @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("bcid",bcid);
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION),BusinessProcessHandler.MERGE_COLLABORATION_GROUPS_LOCAL_PATH, queryParams,body,delegateId);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/collaboration-groups/merge/local")
    public Response mergeCollaborationGroupsLocal(@Context HttpHeaders headers,
                                                  String body,
                                                  @QueryParam("bcid") String bcid) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("bcid",bcid);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.MERGE_COLLABORATION_GROUPS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.MERGE_COLLABORATION_GROUPS_LOCAL_PATH, businessProcessServiceUri.toString(),body, headersToSend ,_frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /documents/expected-orders   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/documents/expected-orders")
    public Response getExpectedOrders(@Context HttpHeaders headers,
                                                             @QueryParam("forAll") @DefaultValue("false")  Boolean forAll,
                                                             @QueryParam("unShippedOrderIds") List<String> unShippedOrderIds) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("forAll",forAll.toString());
        queryParams.put("unShippedOrderIds",getStringQueryParam(unShippedOrderIds));
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_EXPECTED_ORDERS_LOCAL_PATH, queryParams,MergeOption.ListResults);
    }

    @GET
    @Path("/documents/expected-orders/local")
    public Response getExpectedOrdersLocal(@Context HttpHeaders headers,
                                                                  @QueryParam("forAll") @DefaultValue("false") Boolean forAll,
                                                                  @QueryParam("unShippedOrderIds") List<String> unShippedOrderIds) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("forAll",forAll.toString());
        queryParams.put("unShippedOrderIds",getStringQueryParam(unShippedOrderIds));
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_EXPECTED_ORDERS_PATH,queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_EXPECTED_ORDERS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl);
    }
    /************************************   /documents/expected-orders - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/overall")
    public Response getStatistics(@Context HttpHeaders headers,
                                  @QueryParam("partyId") String partyId,
                                  @QueryParam("role") @DefaultValue("SELLER") String role) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get document xml content");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_STATISTICS_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.OverallStatistics);
    }

    @GET
    @Path("/statistics/overall/local")
    public Response getStatisticsLocal(@Context HttpHeaders headers,
                                       @QueryParam("partyId") String partyId,
                                       @QueryParam("role") @DefaultValue("SELLER") String role) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        queryParams.put("role", role);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_STATISTICS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_STATISTICS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /collaboration-groups/{id}   ****************************************/
    @GET
    @Produces("application/zip")
    @Path("/contracts/create-bundle")
    public void generateContract(@Context HttpHeaders headers,
                                 @QueryParam("orderId") String orderId,
                                 @QueryParam("delegateId") String delegateId,
                                 @Context  HttpServletResponse response) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated update document");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("orderId",orderId);
        businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GENERATE_CONTRACT_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"),delegateId,response);
    }

    @GET
    @Produces("application/zip")
    @Path("/contracts/create-bundle/local")
    public void generateContractLocal(@Context HttpHeaders headers,
                                      @QueryParam("orderId") String orderId,
                                      @Context   HttpServletResponse response) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            return;
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("orderId",orderId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GENERATE_CONTRACT_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("Accept","application/zip");
        try {
            _httpHelper.forwardZipRequest(BusinessProcessHandler.GENERATE_CONTRACT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend ,_frontendServiceUrl,response);
        } catch (Exception e) {
            logger.error("Failed to zip response:",e);
            response.setStatus(Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
    /************************************   /collaboration-groups/{id} - END   ************************************/

    /****************************************   /paymentDone/{orderId}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/paymentDone/{orderId}")
    public Response isPaymentDone(@Context HttpHeaders headers,@PathParam("orderId") String orderId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated is payment done");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.IS_PAYMENT_DONE_LOCAL_PATH, orderId), null,null,delegateId);
    }

    @GET
    @Path("/paymentDone/{orderId}/local")
    public Response isPaymentDoneLocal(@Context HttpHeaders headers, @PathParam("orderId") String orderId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.IS_PAYMENT_DONE_PATH, orderId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.IS_PAYMENT_DONE_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /paymentDone/{orderId} - END   ************************************/

    /****************************************   /paymentDone/{orderId}   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/paymentDone/{orderId}")
    public Response paymentDone(@Context HttpHeaders headers,@PathParam("orderId") String orderId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated payment done");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.PAYMENT_DONE_LOCAL_PATH, orderId), null,null,delegateId);
    }

    @POST
    @Path("/paymentDone/{orderId}/local")
    public Response paymentDoneLocal(@Context HttpHeaders headers, @PathParam("orderId") String orderId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.PAYMENT_DONE_PATH, orderId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.PAYMENT_DONE_LOCAL_PATH, businessProcessServiceUri.toString(),null, headersToSend, _frontendServiceUrl);
    }
    /************************************   /paymentDone/{orderId} - END   ************************************/

    /****************************************   /contract/digital-agreement/{id}   ****************************************/
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contract/digital-agreement/{id}")
    public Response deleteDigitalAgreement(@Context HttpHeaders headers,@PathParam("id") Long id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated delete digital agreement");
        return businessProcessServiceCallWrapper("DELETE",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.DELETE_DIGITAL_AGREEMENT_LOCAL_PATH, id.toString()), null,null,delegateId);
    }

    @DELETE
    @Path("/contract/digital-agreement/{id}/local")
    public Response deleteDigitalAgreementLocal(@Context HttpHeaders headers, @PathParam("id") Long id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.DELETE_DIGITAL_AGREEMENT_PATH, id.toString()), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardDeleteRequestWithStringBody(BusinessProcessHandler.DELETE_DIGITAL_AGREEMENT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /contract/digital-agreement/{id} - END   ************************************/

    /****************************************   /process-instance-groups/{id}/finish   ****************************************/
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/process-instance-groups/{id}/finish")
    public Response finishCollaboration(@Context HttpHeaders headers,@PathParam("id") String id,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated finish collaboration");
        return businessProcessServiceCallWrapper("POST",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.FINISH_COLLABORATION_LOCAL_PATH, id), null,null,delegateId);
    }

    @POST
    @Path("/process-instance-groups/{id}/finish/local")
    public Response finishCollaborationLocal(@Context HttpHeaders headers, @PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.FINISH_COLLABORATION_PATH, id), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardPostRequestWithStringBody(BusinessProcessHandler.FINISH_COLLABORATION_LOCAL_PATH, businessProcessServiceUri.toString(), null,headersToSend, _frontendServiceUrl);
    }
    /************************************   /process-instance-groups/{id}/finish - END   ************************************/

    /****************************************   /processInstance/document/{documentId}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/processInstance/document/{documentId}")
    public Response getProcessInstanceIdForDocument(@Context HttpHeaders headers,@PathParam("documentId") String documentId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get process instance id for document");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION), String.format(BusinessProcessHandler.GET_PROCESS_INSTANCE_ID_FOR_DOCUMENT_LOCAL_PATH, documentId), null,null,delegateId);
    }

    @GET
    @Path("/processInstance/document/{documentId}/local")
    public Response getProcessInstanceIdForDocumentLocal(@Context HttpHeaders headers, @PathParam("documentId") String documentId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        URI businessProcessServiceUri = _httpHelper.buildUri(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, String.format(_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_INSTANCE_ID_FOR_DOCUMENT_PATH, documentId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_INSTANCE_ID_FOR_DOCUMENT_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /processInstance/document/{documentId} - END   ************************************/

    /****************************************   /statistics/response-time-months   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/response-time-months")
    public Response getAverageResponseTimeForMonths(@Context HttpHeaders headers,
                                                @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get average response time for months");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION), BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_FOR_MONTHS_LOCAL_PATH, queryParams,headers.getHeaderString("federationId"), MergeOption.AverageResponseTimeForMonths);
    }

    @GET
    @Path("/statistics/response-time-months/local")
    public Response getAverageResponseTimeForMonthsLocal(@Context HttpHeaders headers,
                                                     @QueryParam("partyId") String partyId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("partyId", partyId);
        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port,_businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_FOR_MONTHS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("federationId", headers.getRequestHeader("federationId").get(0));
        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_AVERAGE_RESPONSE_TIME_FOR_MONTHS_LOCAL_PATH, businessProcessServiceUri.toString(), headersToSend, _frontendServiceUrl);
    }
    /************************************   /statistics/response-time-months - END   ************************************/

    /****************************************   /statistics/fulfilment   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/statistics/fulfilment")
    public Response getFulfilmentStatistics(@Context HttpHeaders headers,@QueryParam("orderId") String orderId,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated payment done");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("orderId", orderId);
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),BusinessProcessHandler.GET_FULFILMENT_STATISTICS_LOCAL_PATH, queryParams,null,delegateId);
    }

    @GET
    @Path("/statistics/fulfilment/local")
    public Response getFulfilmentStatisticsLocal(@Context HttpHeaders headers, @QueryParam("orderId") String orderId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("orderId", orderId);

        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_FULFILMENT_STATISTICS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_FULFILMENT_STATISTICS_LOCAL_PATH, businessProcessServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /statistics/fulfilment - END   ************************************/

    /****************************************   /contracts/terms-and-conditions   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/contracts/terms-and-conditions")
    public Response getTermsAndConditions(@Context HttpHeaders headers,
                                          @QueryParam("sellerPartyId") String sellerPartyId,
                                          @QueryParam("buyerPartyId") String buyerPartyId,
                                          @QueryParam("incoterms") String incoterms,
                                          @QueryParam("tradingTerm") String tradingTerm,
                                          @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated payment done");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if(sellerPartyId != null){
            queryParams.put("sellerPartyId", sellerPartyId);
        }
        if(buyerPartyId != null){
            queryParams.put("buyerPartyId", buyerPartyId);
        }
        if(incoterms != null){
            queryParams.put("incoterms", incoterms);
        }
        if(tradingTerm != null){
            queryParams.put("tradingTerm", tradingTerm);
        }
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),BusinessProcessHandler.GET_TERMS_AND_CONDITIONS_LOCAL_PATH,queryParams,null,headers.getHeaderString("initiatorFederationId"),null,delegateId);
    }

    @GET
    @Path("/contracts/terms-and-conditions/local")
    public Response getTermsAndConditionsLocal(@Context HttpHeaders headers,
                                               @QueryParam("sellerPartyId") String sellerPartyId,
                                               @QueryParam("buyerPartyId") String buyerPartyId,
                                               @QueryParam("incoterms") String incoterms,
                                               @QueryParam("tradingTerm") String tradingTerm) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if(sellerPartyId != null){
            queryParams.put("sellerPartyId", sellerPartyId);
        }
        if(buyerPartyId != null){
            queryParams.put("buyerPartyId", buyerPartyId);
        }
        if(incoterms != null){
            queryParams.put("incoterms", incoterms);
        }
        if(tradingTerm != null){
            queryParams.put("tradingTerm", tradingTerm);
        }

        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_TERMS_AND_CONDITIONS_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());
        headersToSend.add("initiatorFederationId",headers.getHeaderString("initiatorFederationId"));

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_TERMS_AND_CONDITIONS_LOCAL_PATH, businessProcessServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /contracts/terms-and-conditions - END   ************************************/

    /****************************************   /rest/engine/default/history/variable-instance   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/rest/engine/default/history/variable-instance")
    public Response getProcessDetailsHistory(@Context HttpHeaders headers,
                                             @QueryParam("processInstanceIdIn") String processInstanceIdIn,
                                             @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated payment done");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if(processInstanceIdIn != null){
            queryParams.put("processInstanceIdIn", processInstanceIdIn);
        }
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),BusinessProcessHandler.GET_PROCESS_DETAILS_HISTORY_LOCAL_PATH,queryParams,null,delegateId);
    }

    @GET
    @Path("/rest/engine/default/history/variable-instance/local")
    public Response getProcessDetailsHistoryLocal(@Context HttpHeaders headers,
                                                  @QueryParam("processInstanceIdIn") String processInstanceIdIn) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        HashMap<String, String> queryParams = new HashMap<String, String>();
        if(processInstanceIdIn != null){
            queryParams.put("processInstanceIdIn", processInstanceIdIn);
        }

        URI businessProcessServiceUri = _httpHelper.buildUriWithStringParams(_businessProcessHandler.BaseUrl, _businessProcessHandler.Port, _businessProcessHandler.PathPrefix+BusinessProcessHandler.GET_PROCESS_DETAILS_HISTORY_PATH, queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(BusinessProcessHandler.GET_PROCESS_DETAILS_HISTORY_LOCAL_PATH, businessProcessServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /rest/engine/default/history/variable-instance - END   ************************************/

    /************************************************   BUSINESS PROCESS SERVICE - END   ************************************************/

    /***********************************   business-process-service - helper function   ***********************************/
    private Response businessProcessServiceCallWrapper(String method,
                                                       String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String body,
                                                       String initiatorFederationIdHeader,
                                                       String responderFederationIdHeader,
                                                       String delegateId) throws IOException {
        return businessProcessServiceCallWrapper(method,userAccessToken,pathToSendRequest,queryParams,body,null,initiatorFederationIdHeader,responderFederationIdHeader,Arrays.asList(delegateId),null,null);
    }

    private Response businessProcessServiceCallWrapper(String method,
                                                       String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String body,
                                                       String federationIdHeader,
                                                       String delegateId) throws IOException {
        return businessProcessServiceCallWrapper(method,userAccessToken,pathToSendRequest,queryParams,body,federationIdHeader,null,null,Arrays.asList(delegateId),null,null);
    }

    private Response businessProcessServiceCallWrapper(String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       MergeOption mergeOption) throws IOException {
        return businessProcessServiceCallWrapper("GET",userAccessToken,pathToSendRequest,queryParams,null,null,null,null,null,mergeOption,null);
    }

    private Response businessProcessServiceCallWrapper(String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String federationIdHeader,
                                                       HttpServletResponse response) throws IOException {
        return businessProcessServiceCallWrapper("GET",userAccessToken,pathToSendRequest,queryParams,null,federationIdHeader,null,null,null,MergeOption.ProcessInstanceData,response);
    }
    private Response businessProcessServiceCallWrapper(String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String federationIdHeader,
                                                       String delegateId,
                                                       HttpServletResponse response) throws IOException {
        return businessProcessServiceCallWrapper("GET",userAccessToken,pathToSendRequest,queryParams,null,federationIdHeader,null,null,Arrays.asList(delegateId),null,response);
    }


    private Response businessProcessServiceCallWrapper(String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String federationIdHeader,
                                                       MergeOption mergeOption) throws IOException {
        return businessProcessServiceCallWrapper("GET",userAccessToken,pathToSendRequest,queryParams,null,federationIdHeader,null,null,null,mergeOption,null);
    }

    private Response businessProcessServiceCallWrapper(String method,
                                                       String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String body,
                                                       String delegateId) throws IOException {
        return businessProcessServiceCallWrapper(method,userAccessToken,pathToSendRequest,queryParams,body,null,null,null,Arrays.asList(delegateId),null,null);
    }

    private Response businessProcessServiceCallWrapper(String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       List<String> delegateIds) throws IOException {
        return businessProcessServiceCallWrapper("GET",userAccessToken,pathToSendRequest,queryParams,null,null,null,null,delegateIds,MergeOption.ListResults,null);
    }

    private Response businessProcessServiceCallWrapper(String method,
                                                       String userAccessToken,
                                                       String pathToSendRequest,
                                                       HashMap<String, String> queryParams,
                                                       String body,
                                                       String federationIdHeader,
                                                       String initiatorFederationIdHeader,
                                                       String responderFederationIdHeader,
                                                       List<String> delegateIds,
                                                       MergeOption mergeOption,
                                                       HttpServletResponse response) throws IOException {
        // validation check of the authorization header in the local identity service
        if (!_identityLocalHandler.userExist(userAccessToken)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }
        if (queryParams != null) {
            logger.info("query params: " + queryParams.toString());
        }
        // replace the authorization header to the federation identity of the delegate service
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<String, Object>();
        headers.add(HttpHeaders.AUTHORIZATION, _identityFederationHandler.getAccessToken());
        if(federationIdHeader != null){
            headers.add("federationId", federationIdHeader);
        }
        if(initiatorFederationIdHeader != null){
            headers.add("initiatorFederationId",initiatorFederationIdHeader);
        }
        if(responderFederationIdHeader != null){
            headers.add("responderFederationId",responderFederationIdHeader);
        }

        if(response != null){
            headers.add("Accept","application/zip");
        }

        DelegateResponse delegateResponse = null;
        if(method.contentEquals("GET") && delegateIds != null && delegateIds.size() == 1){
            delegateResponse = _httpHelper.sendGetRequestToSingleDelegate(pathToSendRequest, headers, queryParams,delegateIds.get(0),response);
        }
        else if(method.contentEquals("GET") && (delegateIds == null || delegateIds.size() > 1)){
            delegateResponse = _httpHelper.sendGetRequestToAllDelegates(pathToSendRequest, headers, queryParams,mergeOption,response,delegateIds);
        }
        else if(method.contentEquals("POST") && delegateIds != null){
            delegateResponse = _httpHelper.sendPostRequestToSingleDelegate(pathToSendRequest, headers, queryParams,body,delegateIds.get(0));
        }
        else if(method.contentEquals("DELETE") && delegateIds != null){
            delegateResponse = _httpHelper.sendDeleteRequestToSingleDelegate(pathToSendRequest, headers, queryParams,delegateIds.get(0));
        }
        else if(method.contentEquals("PATCH")){
            delegateResponse = _httpHelper.sendPatchRequestToSingleDelegate(pathToSendRequest, headers,queryParams,body,delegateIds.get(0));
        }

        return Response.status(Response.Status.fromStatusCode(delegateResponse.getStatus()))
                .type(MediaType.APPLICATION_JSON)
                .entity(delegateResponse.getData())
                .build();
    }

    //TODO: remove this method
    private String getStringQueryParam(List<String> list){
        StringBuilder queryParam = new StringBuilder();
        int size = list.size();
        for(int i = 0; i < size; i++){
            if(i == size - 1){
                queryParam.append(list.get(i));
            }
            else{
                queryParam.append(list.get(i)).append(",");
            }
        }
        return queryParam.toString();
    }
    /***********************************   business-process-service - helper function - END   ***********************************/

    /***************************************************   IDENTITY SERVICE   ***************************************************/

    /****************************************   /company-settings/{companyID}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/company-settings/{companyID}")
    public Response getSettings(@Context HttpHeaders headers,@PathParam("companyID") Long companyID,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get settings");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),String.format(IdentityHandler.GET_COMPANY_SETTINGS_LOCAL_PATH,companyID), null,null,delegateId);
    }

    @GET
    @Path("/company-settings/{companyID}/local")
    public Response getSettingsLocal(@Context HttpHeaders headers, @PathParam("companyID") Long companyID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        URI identityServiceUri = _httpHelper.buildUriWithStringParams(_identityLocalHandler._baseUrl, _identityLocalHandler._port, _identityLocalHandler._pathPrefix+String.format(IdentityHandler.GET_COMPANY_SETTINGS_PATH,companyID), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(IdentityHandler.GET_COMPANY_SETTINGS_LOCAL_PATH, identityServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /company-settings/{companyID} - END   ************************************/

    /****************************************   /company-settings/{companyID}/negotiation/   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/company-settings/{companyID}/negotiation/")
    public Response getNegotiationSettings(@Context HttpHeaders headers,@PathParam("companyID") Long companyID,@QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get settings");
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),String.format(IdentityHandler.GET_NEGOTIATION_SETTINGS_LOCAL_PATH,companyID), null,null,delegateId);
    }

    @GET
    @Path("/company-settings/{companyID}/negotiation/local")
    public Response getNegotiationSettingsLocal(@Context HttpHeaders headers, @PathParam("companyID") Long companyID) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        URI identityServiceUri = _httpHelper.buildUriWithStringParams(_identityLocalHandler._baseUrl, _identityLocalHandler._port, _identityLocalHandler._pathPrefix+String.format(IdentityHandler.GET_NEGOTIATION_SETTINGS_PATH,companyID), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(IdentityHandler.GET_NEGOTIATION_SETTINGS_LOCAL_PATH, identityServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /company-settings/{companyID}/negotiation/ - END   ************************************/

    /****************************************   /party/{partyId}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/party/{partyId}")
    public Response getParty(@Context HttpHeaders headers, @PathParam("partyId") Long partyId, @QueryParam("includeRoles") boolean includeRoles, @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get settings");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("includeRoles", String.valueOf(includeRoles));
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),String.format(IdentityHandler.GET_PARTY_LOCAL_PATH,partyId), queryParams,null,delegateId);
    }

    @GET
    @Path("/party/{partyId}/local")
    public Response getPartyLocal(@Context HttpHeaders headers,@PathParam("partyId") Long partyId,@QueryParam("includeRoles") boolean includeRoles) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("includeRoles", String.valueOf(includeRoles));

        URI identityServiceUri = _httpHelper.buildUriWithStringParams(_identityLocalHandler._baseUrl, _identityLocalHandler._port, _identityLocalHandler._pathPrefix+String.format(IdentityHandler.GET_PARTY_PATH,partyId), queryParams);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(IdentityHandler.GET_PARTY_LOCAL_PATH, identityServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /party/{partyId} - END   ************************************/

    /****************************************   /party/{partyId}   ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/parties/{partyIds}")
    public Response getParty(@Context HttpHeaders headers, @PathParam("partyIds") List<String> partyIds, @QueryParam("includeRoles") boolean includeRoles, @QueryParam("delegateIds") List<String> delegateIds) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get settings");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("includeRoles", String.valueOf(includeRoles));
        logger.info("called delegateIds:{}",delegateIds);
        return businessProcessServiceCallWrapper(headers.getHeaderString(HttpHeaders.AUTHORIZATION),String.format(IdentityHandler.GET_PARTIES_LOCAL_PATH,getStringQueryParam(partyIds)), queryParams,delegateIds);
    }

    @GET
    @Path("/parties/{partyIds}/local")
    public Response getPartyLocal(@Context HttpHeaders headers,@PathParam("partyIds") List<String> partyIds,@QueryParam("includeRoles") boolean includeRoles) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("includeRoles", String.valueOf(includeRoles));

        URI identityServiceUri = _httpHelper.buildUriWithStringParams(_identityLocalHandler._baseUrl, _identityLocalHandler._port, _identityLocalHandler._pathPrefix+String.format(IdentityHandler.GET_PARTIES_PATH,getStringQueryParam(partyIds)), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(IdentityHandler.GET_PARTIES_LOCAL_PATH, identityServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /party/{partyId} - END   ************************************/

    /****************************************   /person/{personId}  ****************************************/
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/person/{personId}")
    public Response getPerson(@Context HttpHeaders headers, @PathParam("personId") Long personId, @QueryParam("delegateId") String delegateId) throws JsonParseException, JsonMappingException, IOException {
        logger.info("called federated get settings");
        HashMap<String, String> queryParams = new HashMap<String, String>();
        return businessProcessServiceCallWrapper("GET",headers.getHeaderString(HttpHeaders.AUTHORIZATION),String.format(IdentityHandler.GET_PERSON_LOCAL_PATH,personId), queryParams,null,delegateId);
    }

    @GET
    @Path("/person/{personId}/local")
    public Response getPersonLocal(@Context HttpHeaders headers,@PathParam("personId") Long personId) throws JsonParseException, JsonMappingException, IOException {
        if (!_identityFederationHandler.userExist(headers.getHeaderString(HttpHeaders.AUTHORIZATION))) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        URI identityServiceUri = _httpHelper.buildUriWithStringParams(_identityLocalHandler._baseUrl, _identityLocalHandler._port, _identityLocalHandler._pathPrefix+String.format(IdentityHandler.GET_PERSON_PATH,personId), null);

        MultivaluedMap<String, Object> headersToSend = new MultivaluedHashMap<String, Object>();
        headersToSend.add(HttpHeaders.AUTHORIZATION, _identityLocalHandler.getAccessToken());

        return _httpHelper.forwardGetRequest(IdentityHandler.GET_PERSON_LOCAL_PATH, identityServiceUri.toString(),headersToSend, _frontendServiceUrl);
    }
    /************************************   /person/{personId} - END   ************************************/

    /***************************************************   IDENTITY SERVICE - END   ***************************************************/
}
