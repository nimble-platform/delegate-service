package eu.nimble.service.delegate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class IndexingServiceResult {
	private int totalElements;
	private int totalPages;
	private int pageSize;
	private int currentPage;
	private List <JsonObject> result;
	private JsonObject facets;
	
	private ArrayList<ServiceEndpoint> endpointsArray;
	private Map<ServiceEndpoint, LinkedList<JsonObject>> resultsPerEndpoint;
	
	public IndexingServiceResult(int rows, int start) {
		totalElements = 0;
		totalPages = 0;
		pageSize = rows;
    	currentPage = start;
    	result = new LinkedList<JsonObject>();
    	facets = null;
    	
    	endpointsArray = new ArrayList<ServiceEndpoint>();
    	resultsPerEndpoint = new LinkedHashMap<ServiceEndpoint, LinkedList<JsonObject>>();
    
	}
	
	public void addToTotalElements(int count) {
		this.totalElements += count;
	}
	
	public void addEndpointResults(ServiceEndpoint endpoint, JsonArray resultArray) {
		LinkedList<JsonObject> resultArrayInList = new LinkedList<JsonObject>();
		for (JsonElement element : resultArray) {
			JsonObject elementAsObj = element.getAsJsonObject();
			elementAsObj.addProperty("sourceIndexingServiceUrl", endpoint.getIndexingServiceUrl());
			elementAsObj.addProperty("nimbleInstanceName", endpoint.getAppName());
			resultArrayInList.add(elementAsObj);
		}
		this.endpointsArray.add(endpoint);
		this.resultsPerEndpoint.put(endpoint, resultArrayInList);
	}
	
	private void addResult(JsonObject resultToAdd) {
		this.result.add(resultToAdd);
	}
	
	public int getPageSize() {
		return this.pageSize;
	}
	
	public void addFacets(JsonElement facetsToAddElement) {
		if (facetsToAddElement == null) {
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
			if (internalFieldName != null && internalFieldName == fieldName) {
				return true;
			}
		}
		return false;
	}
	
	private JsonElement getFacetEntryLabelElement(JsonArray facetEntryArray, String label) {
		for (JsonElement entryElement : facetEntryArray) {
			JsonObject entryObj = entryElement.getAsJsonObject();
			String internalLabel = entryObj.get("label").getAsString();
			if (internalLabel != null && internalLabel == label) {
				return entryObj;
			}
		}
		return null;
	}
	
	
	public Map<String, Object> getFinalResult() {
		Map<String, Object> aggregatedResults = new LinkedHashMap<String, Object>();
		
		this.totalPages = (int) Math.ceil(((double)this.totalElements)/ this.pageSize);
		
		// merge results based on the data that was added to this object 
		int index = 0;
    	while (!endpointsArray.isEmpty()) {
    		ServiceEndpoint endpoint = endpointsArray.get(index);
    		LinkedList<JsonObject> results = resultsPerEndpoint.get(endpoint);
    		if (!results.isEmpty()) {
    			addResult(results.removeFirst());
    			index = (index+1) % endpointsArray.size();
    		}
    		else {
    			endpointsArray.remove(index);
    			index = index % endpointsArray.size();
    		}
    	}
		
		aggregatedResults.put("totalElements", this.totalElements);
		aggregatedResults.put("totalPages", this.totalPages);
		aggregatedResults.put("pageSize", this.pageSize);
		aggregatedResults.put("currentPage", this.currentPage);
		aggregatedResults.put("result", this.result);
		aggregatedResults.put("facets", this.facets);
		
		return aggregatedResults;
	}
}
