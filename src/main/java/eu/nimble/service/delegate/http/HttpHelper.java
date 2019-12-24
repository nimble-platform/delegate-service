package eu.nimble.service.delegate.http;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response.Status;

import eu.nimble.service.delegate.businessprocess.BusinessProcessHandler;
import eu.nimble.service.delegate.businessprocess.MergeOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.nimble.service.delegate.eureka.EurekaHandler;
import eu.nimble.service.delegate.eureka.ServiceEndpoint;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

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
        httpClient = ClientBuilder.newClient().property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND,true);
        this.eurekaHandler = eurekaHandler;
    }

    public URI buildUri(String host, int port, String path, HashMap<String, List<String>> queryParams) {
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

    public URI buildUriWithStringParams(String host, int port, String path, HashMap<String, String> queryParams) {
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

    public void forwardZipRequest(String from, String to, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse, HttpServletResponse httpServletResponse) throws Exception {
        logger.info("got a ZIP request to endpoint " + from + ", forwarding it to " + to);
        logger.info("ForwardZip ServletResponse is null: {}",httpServletResponse == null);

        Builder builder = httpClient.target(to).request();
        if (headers != null) {
            builder.headers(headers);
        }
        Response response = builder.get();
        logger.info("ForwardZip response: {}",response.getStatus());
        httpServletResponse.setHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setHeader("Access-Control-Allow-Credentials", "true");
        httpServletResponse.setHeader("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, federationId");
        httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH");

        if (response.getStatus() >= 200 && response.getStatus() <= 300) {
            InputStream data = response.readEntity(InputStream.class);
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = data.read(buffer)) != -1) {
                httpServletResponse.getOutputStream().write(buffer, 0, bytesRead);
            }
            logger.info("ForwardZip output stream is ready");
            httpServletResponse.setStatus(Status.OK.getStatusCode());

//            return Response.status(Status.OK)
//                    .entity(data)
//                    .type(MediaType.APPLICATION_JSON)
//                    .header("frontendServiceUrl", frontendServiceUrlToPutInResponse)
//                    .build();
        }
        else {
            logger.info("ForwardZip failed: {}",response.getEntity().toString());
            httpServletResponse.setStatus(response.getStatus());
        }
        try {
            httpServletResponse.flushBuffer();
        }catch (Exception e){
            logger.error("error while flushing buffer:",e);
        }

    }

    public Response forwardPatchRequest(String from, String to,String body, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse) {
        logger.info("got a PATCH request to endpoint " + from + ", forwarding it to " + to);

        Builder builder = httpClient.target(to).request();
        if (headers != null) {
            builder.headers(headers);
        }
        Response response = builder.method("PATCH",Entity.json(body == null ? "" : body));

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

    public Response forwardPostRequestWithStringBody(String from, String to, String body, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse) {
        logger.info("got a POST request to endpoint " + from + ", forwarding it to " + to);

        Response response = httpClient.target(to).request().headers(headers).post(body == null ? null: Entity.json(body));
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

    public Response forwardDeleteRequestWithStringBody(String from, String to, MultivaluedMap<String, Object> headers, String frontendServiceUrlToPutInResponse) {
        logger.info("got a DELETE request to endpoint " + from + ", forwarding it to " + to);

        Response response = httpClient.target(to).request().headers(headers).delete();
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
            // Prepare the destination URL
            UriBuilder uriBuilder = UriBuilder.fromUri("");
            uriBuilder.scheme("http");
            // add all query params to the request
            if (queryParams != null) {
                for (Entry<String, List<String>> queryParam : queryParams.entrySet()) {
                    for (String paramValue : queryParam.getValue()) {
                        uriBuilder.queryParam(queryParam.getKey(), paramValue);
                    }
                }
            }
            URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().headers(headers).async().get();
            futureList.add(result);
        }
        return getResponseListFromAllDelegates(endpointList, futureList);
    }

    public String sendGetRequestToAllDelegates(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams, MergeOption mergeOption,HttpServletResponse response) {
        logger.info("send get requests to all delegates");
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        List<Future<Response>> futureList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : endpointList) {
            // Prepare the destination URL
            UriBuilder uriBuilder = UriBuilder.fromUri("");
            uriBuilder.scheme("http");
            // add all query params to the request
            if (queryParams != null) {
                for (Entry<String, String> queryParam : queryParams.entrySet()) {
                    uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
                }
            }
            URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

            logger.info("sending the request to " + endpoint.toString() + "...");
            Future<Response> result = httpClient.target(uri.toString()).request().headers(headers).async().get();
            futureList.add(result);
        }
        if(mergeOption == MergeOption.BooleanResults){
            return BusinessProcessHandler.mergeBooleanResults(futureList);
        }
        else if(mergeOption == MergeOption.DoubleResults){
            return BusinessProcessHandler.mergeDoubleResults(futureList);
        }
        else if(mergeOption == MergeOption.AverageResponseTimeForMonths){
            return BusinessProcessHandler.mergeAverageResponseTimeForMonths(futureList);
        }
        else if(mergeOption == MergeOption.RatingSummaries){
            return BusinessProcessHandler.mergeRatingSummaries(futureList);
        }
        else if(mergeOption == MergeOption.CollaborationGroups){
            return BusinessProcessHandler.mergeCollaborationGroups(endpointList, futureList);
        }
        else if(mergeOption == MergeOption.ProcessInstanceGroupFilter){
            return BusinessProcessHandler.mergeProcessInstanceGroupFilters(futureList);
        }
        else if(mergeOption == MergeOption.IndividualRatingsAndReviews){
            return BusinessProcessHandler.mergeIndividualRatingsAndReviews(futureList);
        }
        else if(mergeOption == MergeOption.ProcessInstanceData){
            BusinessProcessHandler.mergeProcessInstanceData(endpointList,futureList,response);
        }
        else if(mergeOption == MergeOption.OverallStatistics){
            return BusinessProcessHandler.mergeOverallStatistics(futureList);
        }
        return "";
    }

    public String sendGetRequestToSingleDelegate(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams, String delegateId, HttpServletResponse servletResponse) {
        logger.info("send get requests to single delegate: {}",delegateId);
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        Future<Response> response = null;

        for (ServiceEndpoint endpoint : endpointList) {
            if(endpoint.getId().contentEquals(delegateId)){
                // Prepare the destination URL
                UriBuilder uriBuilder = UriBuilder.fromUri("");
                uriBuilder.scheme("http");
                // add all query params to the request
                if (queryParams != null) {
                    for (Entry<String, String> queryParam : queryParams.entrySet()) {
                        uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
                    }
                }
                URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

                logger.info("sending the request to " + endpoint.toString() + "...");
                response = httpClient.target(uri.toString()).request().headers(headers).async().get();
                return getResponseFromSingleDelegate( response,endpoint,servletResponse);
            }
        }
        return null;
    }

    public String sendPatchRequestToSingleDelegate(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams,String body, String delegateId) {
        logger.info("send patch request to single delegate: {}",delegateId);
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        Future<Response> response = null;

        for (ServiceEndpoint endpoint : endpointList) {
            if(endpoint.getId().contentEquals(delegateId)){
                // Prepare the destination URL
                UriBuilder uriBuilder = UriBuilder.fromUri("");
                uriBuilder.scheme("http");
                // add all query params to the request
                if (queryParams != null) {
                    for (Entry<String, String> queryParam : queryParams.entrySet()) {
                        uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
                    }
                }
                URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

                logger.info("sending the request to " + endpoint.toString() + "...");
                response = httpClient.target(uri.toString()).request().headers(headers).async().method("PATCH",Entity.json(body == null ? "" : body));
                return getResponseFromSingleDelegate( response,endpoint);
            }
        }
        return null;
    }

    public String sendDeleteRequestToSingleDelegate(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams, String delegateId) {
        logger.info("send get requests to single delegates");
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        Future<Response> response = null;

        for (ServiceEndpoint endpoint : endpointList) {
            if(endpoint.getId().contentEquals(delegateId)){
                // Prepare the destination URL
                UriBuilder uriBuilder = UriBuilder.fromUri("");
                uriBuilder.scheme("http");
                // add all query params to the request
                if (queryParams != null) {
                    for (Entry<String, String> queryParam : queryParams.entrySet()) {
                        uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
                    }
                }
                URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();

                logger.info("sending the request to " + endpoint.toString() + "...");
                response = httpClient.target(uri.toString()).request().headers(headers).async().delete();
                return getResponseFromSingleDelegate( response,endpoint);
            }
        }
        return null;
    }

//    public String sendPatchRequestToSingleDelegate(String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams, String body, String delegateId) {
//        logger.info("send patch request to single delegates");
//        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
//        Future<Response> response = null;
//
//        for (ServiceEndpoint endpoint : endpointList) {
//            if(endpoint.getId().contentEquals(delegateId)){
//                // Prepare the destination URL
//                UriBuilder uriBuilder = UriBuilder.fromUri("");
//                uriBuilder.scheme("http");
//                // add all query params to the request
//                if (queryParams != null) {
//                    for (Entry<String, String> queryParam : queryParams.entrySet()) {
//                        uriBuilder.queryParam(queryParam.getKey(), queryParam.getValue());
//                    }
//                }
//                URI uri = uriBuilder.host(endpoint.getHostName()).port(endpoint.getPort()).path(urlPath).build();
//
//                logger.info("sending the request to " + endpoint.toString() + "...");
//                response = httpClient.target(uri.toString()).request().headers(headers).async().post(Entity.json(body));
//                break;
//            }
//        }
//        return getResponseFromSingleDelegate( response);
//    }

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

    public String sendPostRequestToSingleDelegate( String urlPath, MultivaluedMap<String, Object> headers, HashMap<String, String> queryParams,String body, String delegateId) {
        logger.info("send post requests to single delegate");
        List<ServiceEndpoint> endpointList = eurekaHandler.getEndpointsFromEureka();
        Future<Response> response = null;

        for (ServiceEndpoint endpoint : endpointList) {
            if(endpoint.getId().contentEquals(delegateId)){
                URI uri = buildUriWithStringParams(endpoint.getHostName(), endpoint.getPort(), urlPath, queryParams);
                logger.info("sending the request to " + endpoint.toString() + "...");
                Future<Response> result = httpClient.target(uri.toString()).request().headers(headers).async().post(Entity.json(body));
                return getResponseFromSingleDelegate( result,endpoint);
            }
        }

        return null;
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

    public String getResponseFromSingleDelegate(Future<Response> response, ServiceEndpoint endpoint, HttpServletResponse servletResponse) {
        String data = null;
        try {
            Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (res.getStatus() > 300) {
                logger.warn("got failure status code " + res.getStatus() + " message: "+res.getEntity().toString()+" from appName:" + endpoint.getAppName() +
                        " (" + endpoint.getHostName() +
                        ":" + endpoint.getPort() + ")");
            }
            if(servletResponse == null){
                data = res.readEntity(String.class);
            }
            else {
                servletResponse.setHeader("Access-Control-Allow-Origin", "*");
                servletResponse.setHeader("Access-Control-Allow-Credentials", "true");
                servletResponse.setHeader("Access-Control-Allow-Headers", "origin, content-type, accept, authorization, federationId");
                servletResponse.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH");

                ZipOutputStream zos = new ZipOutputStream(servletResponse.getOutputStream());
                InputStream inputStream = null;
                ZipInputStream zipInputStream = null;
                try {
                    inputStream = res.readEntity(InputStream.class);

                    zipInputStream = new ZipInputStream(inputStream);
                    ZipEntry zipEntry;
                    while ((zipEntry = zipInputStream.getNextEntry()) != null){
                        zos.putNextEntry(zipEntry);
                    }
                    servletResponse.flushBuffer();
                }
                catch (Exception e){
                    logger.error("Failed to create a zip:",e);
                }
                finally {
                    if(inputStream != null){
                        inputStream.close();
                    }
                    if(zipInputStream != null){
                        zipInputStream.close();
                    }
                    zos.close();
                }
            }
        } catch(Exception e) {
            logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
                    " appName:" + endpoint.getAppName() +
                    " (" + endpoint.getHostName() +
                    ":" + endpoint.getPort() + ") - " +
                    e.getMessage());
        }
//        logger.info("aggregated results: \n" + resList.toString());
        return data;
    }

    public String getResponseFromSingleDelegate(Future<Response> response, ServiceEndpoint endpoint) {
        String data = null;
        try {
            Response res = response.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (res.getStatus() > 300) {
                logger.warn("got failure status code " + res.getStatus() + " message: "+res.getEntity().toString()+" from appName:" + endpoint.getAppName() +
                        " (" + endpoint.getHostName() +
                        ":" + endpoint.getPort() + ")");
            }
            data = res.readEntity(String.class);
        } catch(Exception e) {
            logger.warn("Failed to send request to eureka endpoint: id: " +  endpoint.getId() +
                    " appName:" + endpoint.getAppName() +
                    " (" + endpoint.getHostName() +
                    ":" + endpoint.getPort() + ") - " +
                    e.getMessage());
        }
//        logger.info("aggregated results: \n" + resList.toString());
        return data;
    }
}
