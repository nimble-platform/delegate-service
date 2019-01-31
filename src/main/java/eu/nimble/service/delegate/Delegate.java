package eu.nimble.service.delegate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.appinfo.providers.EurekaConfigBasedInstanceInfoProvider;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.net.URI;

/**
 * Delegate service.
 *
 * Created by Idan Zach (idanz@il.ibm.com) 01/30/2019.
 */
@ApplicationPath("/")
@Path("/")
public class Delegate implements ServletContextListener {
    private static final int REQ_TIMEOUT_SEC = 3;

    private static Logger logger = LogManager.getLogger(Delegate.class);

    private static ApplicationInfoManager applicationInfoManager;
    private static EurekaClient eurekaClient;
    private static String vipAddress = "delegate.ibm.com";
    private static String urlPath = "/delegate";
    private static int includeErrorRes = 1;
    private static Client httpClient;

    @GET
    @Path("/serve")
    public Response serve(
        @DefaultValue("0") @QueryParam("local") int local) {
        // local indicates whether it is a local request or delegation request
        // value of 0 means local request, so the service should just return a local response
        // value of 1 means delegate request, so the service should call all the services registered in Eureka
        if (local == 1) {
            return Response.status(Response.Status.OK)
                           .type(MediaType.TEXT_PLAIN)
                           .entity("Hello from " + applicationInfoManager.getInfo().getId() + " " + new Date())
                           .build();
        }

        // Call all the services registered in Eureka with local=1
        return sendRequestToAllServices();
    }

    @GET
    @Path("eureka")
    @Produces({ MediaType.APPLICATION_JSON })
    // Return the Delegate services registered in Eureka server (Used for debug)
    public Response eureka() {
        List<ServiceEndpoint> epList = getEndpointsFromEureka();
        return Response.status(Response.Status.OK).entity(epList).build();
    }

    public void contextInitialized(ServletContextEvent arg0) 
    {
        String param = arg0.getServletContext().getInitParameter("vipAddress");
        if (param != null) {
            vipAddress = param;
        }

        param = arg0.getServletContext().getInitParameter("urlPath");
        if (param != null) {
            urlPath = param;
        }

        param = arg0.getServletContext().getInitParameter("includeErrorResponse");
        if (param != null && param != "1") {
            try {
                includeErrorRes = Integer.parseInt(param);
            } catch (NumberFormatException e) {
                logger.warn("Invalid argument for includeErrorResponse - " + e);
            }
        }

        logger.info("Delegate service is being initialized (vipAddress = " + vipAddress +
                    ", urlPath = " + urlPath +
                    ", includeErrorResponse = " + includeErrorRes +
                    ")...");
        httpClient = ClientBuilder.newClient();

        if (!initEureka()) {
            logger.error("Failed to initialize Eureka client");
            return;
        }

        logger.info("Delegate service has been initialized");
    }

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.DOWN);
        logger.info("Delegate service has been destroyed");
    }

    // Initializes Eureka client and registers the service with the Eureka server
    private boolean initEureka() {
        try {
           MyDataCenterInstanceConfig instanceConfig = new MyDataCenterInstanceConfig();
           InstanceInfo instanceInfo = new EurekaConfigBasedInstanceInfoProvider(instanceConfig).get();
           applicationInfoManager = new ApplicationInfoManager(instanceConfig, instanceInfo);
           eurekaClient = new DiscoveryClient(applicationInfoManager, new DefaultEurekaClientConfig());
           applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.UP);
           logger.info("Delegate has been registered in Eureka: " +
                       instanceInfo.getAppName() + "/" +
                       instanceInfo.getVIPAddress() + "(" +
                       instanceInfo.getId() +") " +
                       instanceInfo.getHostName() + ":" +
                       instanceInfo.getPort());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // Returns a list of Delegate services registered in Eureka server
    private List<ServiceEndpoint> getEndpointsFromEureka() {
        List<ServiceEndpoint> dlgList = new ArrayList<ServiceEndpoint>();
        List<InstanceInfo> instsList = eurekaClient.getInstancesByVipAddress(vipAddress, false);
        for (InstanceInfo ii : instsList) {
           // Filter out services that are not UP
           if (ii.getStatus() == InstanceInfo.InstanceStatus.UP) {
               dlgList.add(new ServiceEndpoint(ii.getId(), ii.getHostName(), ii.getPort()));
           }
        }
        return dlgList;
    }

    // Sends the request to all the Delegate services which are registered in the Eureka server
    private Response sendRequestToAllServices() {
        List<ServiceEndpoint> epList = getEndpointsFromEureka();
        List<Future<Response>> fuList = new ArrayList<Future<Response>>();

        for (ServiceEndpoint endpoint : epList) {
            // Prepare the destination URL
            UriBuilder uriBuilder = UriBuilder.fromUri("");
            uriBuilder.scheme("http").queryParam("local", "1");;
            URI uri = uriBuilder.host(endpoint.hostName).port(endpoint.port).path(urlPath).build();
            Future<Response> fuRes = httpClient.target(uri.toString()).request().async().get();
            fuList.add(fuRes);
        }

        // Wait (one by one) for the responses from all the services
        List<ServiceResponse> resList = new ArrayList<ServiceResponse>();
        for(int i = 0; i< fuList.size(); i++) {
            Future<Response> fuRes = fuList.get(i);
            ServiceEndpoint endpoint = epList.get(i);
            try {
                Response res = fuRes.get(REQ_TIMEOUT_SEC, TimeUnit.SECONDS);
                String data = res.readEntity(String.class);
                resList.add(new ServiceResponse(endpoint.id, data, res.getStatus()));
            } catch(Exception e) {
                logger.warn("Failed to call service: " +  endpoint.id +
                            " (" + endpoint.hostName +
                            ":" + endpoint.port + ") - " +
                            e.getMessage());

                if (includeErrorRes == 1) {
                    resList.add(new ServiceResponse(endpoint.id, e.getMessage(), 503));
                }
            }
        }
        return Response.status(Response.Status.OK)
                       .type(MediaType.APPLICATION_JSON)
                       .entity(resList)
                       .build();
    }

}
