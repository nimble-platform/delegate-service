package eu.nimble.service.delegate.indexing;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.nimble.service.delegate.eureka.EurekaHandler;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;
import eu.nimble.service.delegate.http.HttpHelper;

/**
 * Indexing Service Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 07/30/2019.
 */
public class IndexingHandler {
	private static Logger logger = LogManager.getLogger(IndexingHandler.class);
    
    private static String SERVICE_URL = "INDEXING_SERVICE_BASE_URL";
    private static String SERVICE_PORT = "INDEXING_SERVICE_PORT";
	
    public static String GET_ITEM_FIELDS_PATH = "/item/fields";
    public static String GET_ITEM_FIELDS_LOCAL_PATH = "/item/fields/local";
    public static String GET_PARTY_FIELDS_PATH = "/party/fields";
    public static String GET_PARTY_FIELDS_LOCAL_PATH = "/party/fields/local";
    public static String POST_ITEM_SEARCH_PATH = "/item/search";
    public static String POST_ITEM_SEARCH_LOCAL_PATH = "/item/search/local";
    public static String POST_PARTY_SEARCH_PATH = "/party/search";
    public static String POST_PARTY_SEARCH_LOCAL_PATH = "/party/search/local";
    
    public String BaseUrl;
    public int Port;
    public String PathPrefix;
    
    private EurekaHandler _eurekaHandler;
    private HttpHelper _httpHelper;
    private ObjectMapper _mapper;
    private JsonParser _jsonParser;
    
    public IndexingHandler(HttpHelper httpHelper, EurekaHandler eurekaHandler) {
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
    	this._eurekaHandler = eurekaHandler;
    	
    	_mapper = new ObjectMapper();
    	_jsonParser = new JsonParser();
    	
    	logger.info("Service Handler is being initialized with base url = " + BaseUrl + ", path prefix = " + PathPrefix + ", port = " + Port + "...");
    }
    
    @SuppressWarnings("unchecked")
  	public HashMap<ServiceEndpoint, String> getPostItemSearchAggregatedResults(MultivaluedMap<String, Object> headers, Map<String, Object> body) throws JsonParseException, JsonMappingException, IOException {
      	int requestedPageSize = Integer.parseInt(body.get("rows").toString()); // save it before manipulating
      	// manipulate body in order to get results from all delegates.
      	List<ServiceEndpoint> endpointList = _eurekaHandler.getEndpointsFromEureka();
      	body.put("rows", 0); // send dummy request just to get totalElements fields from all delegates
      	HashMap<ServiceEndpoint, String> dummyResultList = _httpHelper.sendPostRequestToAllDelegates(endpointList, POST_ITEM_SEARCH_LOCAL_PATH, headers, body);
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
      		Map<String, Object> json = _mapper.readValue(entry.getValue(), Map.class);
      		int totalElementForEndpoint = Integer.parseInt(json.get("totalElements").toString());
      		totalElementPerEndpoint.put(entry.getKey(), totalElementForEndpoint);
      		sumTotalElements += totalElementForEndpoint;
      	}
      	if (sumTotalElements <= requestedPageSize || requestedPageSize == 0 || endpointList.size()==1) {
      		body.put("rows", requestedPageSize); 
      		return _httpHelper.sendPostRequestToAllDelegates(endpointList, POST_ITEM_SEARCH_LOCAL_PATH, headers, body);
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
      		aggregatedResults.putAll(_httpHelper.sendPostRequestToAllDelegates(listForRequest, POST_ITEM_SEARCH_LOCAL_PATH, headers, body));
      		numOfRowsAggregated += endpointRows;
      	}
      	
      	return aggregatedResults;
      }
    
    // if field name exists in more than one instance, putting the entry just once, ignoring doc count field
    public List<Map<String, Object>> mergeGetResponsesByFieldName(HashMap<ServiceEndpoint, String> resultList) {
    	logger.info("merging results of GET request based on field name");
    	List<Map<String, Object>> aggregatedResults = new LinkedList<Map<String, Object>>();
    	
    	for (String results : resultList.values()) {
    		if (results == null || results.isEmpty()) {
				continue;
			}
    		List<Map<String, Object>> json;
			try {
				json = _mapper.readValue(results, _mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
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
    
    public Set<String> getLocalFieldNamesFromIndexingSerivce(String indexingServiceRelativePath) {
    	URI uri = _httpHelper.buildUri(BaseUrl, Port, indexingServiceRelativePath, null);
        logger.info("sending a request to " + uri.toString() + " in order to clean non existing field names");
        
        Response response = _httpHelper.sendGetRequest(uri, null);
        if (response.getStatus() >= 400) { // we had an issue, we can't modify the body without any response
        	logger.warn("get error when calling GET '/item/fields' in indexing service");
        	return new HashSet<String>();
        }
        
        String data = response.readEntity(String.class);
        Set<String> localFieldNames = new HashSet<String>();
		try {
			List<Map<String, Object>> json = _mapper.readValue(data, _mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
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
    
	public boolean fqListContainNonLocalFieldName(Map<String, Object> body, Set<String> localFieldNames) {
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
    public void removeNonExistingFieldNamesFromBody(Map<String, Object> body, Set<String> localFieldNames) {
    	if (body.get("facet") == null) {
    		return;
    	}
    	JsonObject facetJsonObject = _jsonParser.parse(body.get("facet").toString()).getAsJsonObject();
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
}
