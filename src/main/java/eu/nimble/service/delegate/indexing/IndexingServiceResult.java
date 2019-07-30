package eu.nimble.service.delegate.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.nimble.service.delegate.eureka.ServiceEndpoint;

/**
 * Indexing Service Result
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 06/12/2019.
 */

public class IndexingServiceResult {
	private static Logger logger = LogManager.getLogger(IndexingServiceResult.class);
	
	// response fields
	private int totalElements;
	private int totalPages;
	private int pageSize;
	private int currentPage;
	private JsonArray result;
	private JsonObject facets;
	// response fields end
	
	private JsonParser jsonParser;
	private ObjectMapper mapper;
	
	private ArrayList<ServiceEndpoint> endpointsArray;
	private Map<ServiceEndpoint, LinkedList<JsonObject>> resultsPerEndpoint;
	
	public IndexingServiceResult(int rows, int start) {
		totalElements = 0;
		totalPages = 0;
		pageSize = rows;
    	currentPage = start;
    	result = new JsonArray();
    	facets = null;
    	
    	jsonParser = new JsonParser();
    	mapper = new ObjectMapper();
    	
    	endpointsArray = new ArrayList<ServiceEndpoint>();
    	resultsPerEndpoint = new LinkedHashMap<ServiceEndpoint, LinkedList<JsonObject>>();
	}
	
	public void addEndpointResponse(ServiceEndpoint endpoint, String responseJson, boolean localInstance) {
		logger.info("adding response from instance " + endpoint.getAppName() + ", localInstance = " + localInstance);
		if (responseJson == null || responseJson.isEmpty()) {
			return;
		}
		JsonObject jsonObject = jsonParser.parse(responseJson).getAsJsonObject();
		// summarize totalElements
		this.totalElements += jsonObject.get("totalElements").getAsInt();
		// prepare result field for merge later while calculating final result
		this.addEndpointResults(endpoint, jsonObject.get("result").getAsJsonArray(), localInstance);
		// merge facets
		this.addFacets(jsonObject.get("facets"));
	}
	
	private void addEndpointResults(ServiceEndpoint endpoint, JsonArray resultArray, boolean localInstance) {
		LinkedList<JsonObject> resultArrayInList = new LinkedList<JsonObject>();
		for (JsonElement element : resultArray) {
			JsonObject elementAsObj = element.getAsJsonObject();
			elementAsObj.addProperty("sourceFrontendServiceUrl", endpoint.getFrontendServiceUrl());
			elementAsObj.addProperty("nimbleInstanceName", endpoint.getAppName());
			elementAsObj.addProperty("isFromLocalInstance", localInstance);
			resultArrayInList.add(elementAsObj);
		}
		if (localInstance) {
			this.endpointsArray.add(0, endpoint);
		}
		else {
			this.endpointsArray.add(endpoint);
		}
		this.resultsPerEndpoint.put(endpoint, resultArrayInList);
	}
	
	private void addFacets(JsonElement facetsToAddElement) {
		if (facetsToAddElement == null) {
			return;
		}
		if (!facetsToAddElement.isJsonObject()) {
			return;
		}
		JsonObject facetsToAdd = facetsToAddElement.getAsJsonObject();
		if (facetsToAdd == null) {
			return;
		}
		if (this.facets == null) {
			this.facets= new JsonObject(); 
		}
		for (String facetKey : facetsToAdd.keySet()) {
			JsonElement facetToAddElement = facetsToAdd.get(facetKey);
			// if facet fieldName doesn't exist, add the facet fully to the result list
			JsonElement internalObj = this.facets.get(facetKey);
			if (internalObj == null) { // means this entry doesn't exists in the facets result list
				this.facets.add(facetKey, facetToAddElement);
				continue;
			}
			String facetToAddFieldName = facetToAddElement.getAsJsonObject().get("fieldName").getAsString(); // sanity check, fieldName check
			if (facetFieldNameExist(facetToAddFieldName) == false) {
				this.facets.add(facetKey, facetToAddElement);
				continue;
			}
			// if we reached here, facet fieldName exists, need to merge results
			JsonArray resultEntry = internalObj.getAsJsonObject().get("entry").getAsJsonArray(); // the facet entry array in the result list
			// iterate over the entry array in the facet to add
			for (JsonElement entry : facetToAddElement.getAsJsonObject().get("entry").getAsJsonArray()) {
				JsonObject entryObj = entry.getAsJsonObject();
				String internalLabel = entryObj.get("label").getAsString();
				JsonElement facetLabelInResult = getFacetEntryLabelElement(resultEntry, internalLabel);
				if (facetLabelInResult == null) { // means label doesn't exist in the entry array
					resultEntry.add(entry); // adding entry to result array
				}
				else { // other label exists, need to summarize counters
					int newCount = facetLabelInResult.getAsJsonObject().get("count").getAsInt() + 
									entryObj.get("count").getAsInt();
					facetLabelInResult.getAsJsonObject().addProperty("count", newCount);
				}
			}
		}
	}
	
	private boolean facetFieldNameExist(String fieldName) {
		for (String key : this.facets.keySet()) {
			String internalFieldName = this.facets.get(key).getAsJsonObject().get("fieldName").getAsString();
			if (internalFieldName != null && internalFieldName.equals(fieldName)) {
				return true;
			}
		}
		return false;
	}
	
	private JsonElement getFacetEntryLabelElement(JsonArray facetEntryArray, String label) {
		for (JsonElement entryElement : facetEntryArray) {
			JsonObject entryObj = entryElement.getAsJsonObject();
			String internalLabel = entryObj.get("label").getAsString();
			if (internalLabel != null && internalLabel.equals(label)) {
				return entryObj;
			}
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getFinalResult() throws JsonParseException, JsonMappingException, IOException {
		Map<String, Object> aggregatedResults = new LinkedHashMap<String, Object>();
		if (this.pageSize == 0) {
			this.totalPages = 1;
		} 
		else {
			this.totalPages = (int) Math.ceil(((double)this.totalElements)/ this.pageSize);
		}
		
		// merge results based on the data that was added to this object 
		int index = 0;
    	while (!endpointsArray.isEmpty()) {
    		index = index % endpointsArray.size();
    		ServiceEndpoint endpoint = endpointsArray.get(index);
    		LinkedList<JsonObject> results = resultsPerEndpoint.get(endpoint);
    		if (!results.isEmpty()) {
    			this.result.add(results.removeFirst());
    			index++;
    		}
    		else {
    			endpointsArray.remove(index);
    		}
    	}
		
		aggregatedResults.put("totalElements", this.totalElements);
		aggregatedResults.put("totalPages", this.totalPages);
		aggregatedResults.put("pageSize", this.pageSize);
		aggregatedResults.put("currentPage", this.currentPage);
		
		List<Map<String, Object>> resultList;
		resultList = mapper.readValue(this.result.toString(), mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
		aggregatedResults.put("result", resultList);
		if (facets != null) {
			Map<String, Object> facetsMap = mapper.readValue(this.facets.toString(), Map.class);
			aggregatedResults.put("facets", facetsMap); 
		}
		else {
			aggregatedResults.put("facets", null);
		}
		
		return aggregatedResults;
	}
}
