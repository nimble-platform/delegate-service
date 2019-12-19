package eu.nimble.service.delegate.http;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.nimble.service.delegate.eureka.EurekaHandler;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;

/**
 * Http calls handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 07/30/2019.
 */
public class HttpHelper {
	private static Logger logger = LogManager.getLogger(HttpHelper.class);
	private static final int REQ_TIMEOUT_SEC = 3;
	
	private Client httpClient;
	private EurekaHandler eurekaHandler;
	
	public HttpHelper(EurekaHandler eurekaHandler) {
		httpClient = ClientBuilder.newClient();
		this.eurekaHandler = eurekaHandler;
	}
	
    public URI buildUri(String host, int port, String path, HashMap<String, List<String>> queryParams) {
    	// Prepare the destination URL for the request
        UriBuilder uriBuilder = UriBuilder.fromUri("");
        uriBuilder.scheme("http");
        if (queryParams != null) {
        	for (Entry<String, List<String>> queryParam : queryParams.entrySet()) {
        		for (String paramValue : queryParam.getValue()) {
        			uriBuilder.queryParam(queryParam.getKey(), paramValue);
        		}
        	}
        }
        uriBuilder.host(host).path(path);
        if (port > 0) { // in case the request is sent to nginx, no port is needed (will be set to -1)
        	uriBuilder.port(port);
        }
        return uriBuilder.build();
    }
    
    // forward get request
    public Response forwardGetRequest(String from, String to, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse) {
    	logger.info("got a GET request to endpoint " + from + ", forwarding it to " + to);
        
    	Builder builder = httpClient.target(to).request();
    	if (headers != null) {
    		builder.headers(headers);
    	}
    	Response response = builder.get();
    	
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
        	String data = response.readEntity(String.class);
            return Response.status(Status.OK)
            				.entity(data)
            				.type(MediaType.APPLICATION_JSON)
            				.header("frontendServiceUrl", frontendServiceUrlToPutInResponse)
            				.build();
        }
        else {
        	return response;
        }
    }
    
    // forward post request
    public Response forwardPostRequest(String from, String to, Map<String, Object> body, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse) {
    	logger.info("got a POST request to endpoint " + from + ", forwarding it to " + to + " with body: " + body.toString());
        
        Response response = httpClient.target(to).request().headers(headers).post(Entity.json(body));
        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
        	String data = response.readEntity(String.class);
            return Response.status(Status.OK)
            				.entity(data)
            				.type(MediaType.APPLICATION_JSON)
            				.header("frontendServiceUrl", frontendServiceUrlToPutInResponse)
            				.build();
        }
        else {
        	return response;
        }
    }
    
    public Response sendGetRequest(URI uri, MultivaluedMap<String, Object> headers) {
    	Builder builder = httpClient.target(uri.toString()).request();
    	if (headers != null) {
    		builder.headers(headers);
    	}
    	return builder.get();
    }
    
    public Response sendPostRequest(URI uri, MultivaluedMap<String, Object> headers, Map<String, Object> body) {
    	Builder builder = httpClient.target(uri.toString()).request();
    	if (headers != null) {
    		builder.headers(headers);
    	}
    	return builder.post(Entity.json(body));
    }
    
    // Sends the get request to all the Delegate services which are registered in the Eureka server
    public HashMap<ServiceEndpoint, String> sendGetRequestToAllDelegates(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, List<String>> queryParams) {
    	logger.info("send get requests to all delegates");
    	List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        List<Future<Response>> futureList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : endpointList) {
            URI uri = buildUri(endpoint.getHostName(), endpoint.getPort(), urlPath, queryParams);
            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().headers(headers).async().get();
            futureList.add(result);
        }
        return getResponseListFromAllDelegates(endpointList, futureList);
    }
    
    // Sends the post request to all the Delegate services which are registered in the Eureka server
    public HashMap<ServiceEndpoint, String> sendPostRequestToAllDelegates(List<ServiceEndpoint> endpointList, String urlPath, MultivaluedMap<String, Object> headers, Map<String, Object> body) {
    	logger.info("send post requests to all delegates");
        List<Future<Response>> futureList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : endpointList) {
            URI uri = buildUri(endpoint.getHostName(), endpoint.getPort(), urlPath, null);
            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().headers(headers).async().post(Entity.json(body));
            futureList.add(result);
        }
        return getResponseListFromAllDelegates(endpointList, futureList);
    }
    
    // get responses from all Delegate services which are registered in the Eureka server
    public HashMap<ServiceEndpoint, String> getResponseListFromAllDelegates(List<ServiceEndpoint> endpointList, List<Future<Response>> futureList) {
        // Wait (one by one) for the responses from all the services
        HashMap<ServiceEndpoint, String> resList = new HashMap<ServiceEndpoint, String>();
        for(int i = 0; i< futureList.size(); i++) {
            Future<Response> response = futureList.get(i);
            ServiceEndpoint endpoint = endpointList.get(i);
            logger.info("got response from " + endpoint.toString());
            try {
            	Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
            	if (res.getStatus() > 300) {
            		logger.warn("got failure status code " + res.getStatus() + " from appName:" + endpoint.getAppName() +
                            " (" + endpoint.getHostName() +
                            ":" + endpoint.getPort() + ")");
            		continue;
            	}
                String data = res.readEntity(String.class);
                endpoint.setFrontendServiceUrl(res.getHeaderString("frontendServiceUrl"));
                resList.put(endpoint, data);
            } catch(Exception e) {
                logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
                			" appName:" + endpoint.getAppName() +
                            " (" + endpoint.getHostName() +
                            ":" + endpoint.getPort() + ") - " +
                            e.getMessage());
            }
        }
        logger.info("aggregated results: \n" + resList.toString());
        return resList;
    }
}
