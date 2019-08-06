package eu.nimble.service.delegate.eureka;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.appinfo.providers.EurekaConfigBasedInstanceInfoProvider;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;

/**
 * Eureka Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 07/30/2019.
 */
public class EurekaHandler {
	private static Logger logger = LogManager.getLogger(EurekaHandler.class);
	
	private static ApplicationInfoManager applicationInfoManager;
    private static EurekaClient eurekaClient;
    private static String vipAddress = "eu.nimble.delegate";
	
	 // Initializes Eureka client and registers the service with the Eureka server
    public boolean initEureka() {
        try {
        	logger.info("trying to init eureka client");
        	MyDataCenterInstanceConfig instanceConfig = new MyDataCenterInstanceConfig();
        	InstanceInfo instanceInfo = new EurekaConfigBasedInstanceInfoProvider(instanceConfig).get();
        	applicationInfoManager = new ApplicationInfoManager(instanceConfig, instanceInfo);
        	eurekaClient = new DiscoveryClient(applicationInfoManager, new DefaultEurekaClientConfig());
        	applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.UP);
        	logger.info("Delegate has been registered in Eureka: " +
        				instanceInfo.getAppName() + "/" +
        				instanceInfo.getVIPAddress() + "(" +
        				instanceInfo.getId() +") " +
        				instanceInfo.getHomePageUrl() + ":" +
        				instanceInfo.getPort());
        } catch (Exception e) {
        	logger.error(e.getMessage());
        	e.printStackTrace();
            return false;
        }
        return true;
    }
    
    // Returns a list of Delegate services registered in Eureka server
    public List<ServiceEndpoint> getEndpointsFromEureka() {
        List<ServiceEndpoint> delegateList = new ArrayList<ServiceEndpoint>();
        List<InstanceInfo> instanceList = eurekaClient.getInstancesByVipAddress(vipAddress, false);
        for (InstanceInfo info : instanceList) {
           // Filter out services that are not UP
           if (info.getStatus() == InstanceInfo.InstanceStatus.UP) {
               delegateList.add(new ServiceEndpoint(info.getId(), info.getHomePageUrl(), info.getPort(), info.getAppName()));
           }
        }
        return delegateList;
    } 
	
    public ServiceEndpoint getEndpointByAppName(String appName) {
    	List<InstanceInfo> instanceList = eurekaClient.getInstancesByVipAddressAndAppName(vipAddress, appName, false);
    	for (InstanceInfo info : instanceList) {
    		// Filter out services that are not UP
            if (info.getStatus() == InstanceInfo.InstanceStatus.UP) {
                return new ServiceEndpoint(info.getId(), info.getHomePageUrl(), info.getPort(), info.getAppName());
            }
        }
    	return null;
    }
    
    public String getId() {
    	return applicationInfoManager.getInfo().getId();
    }
    
    public void destroy() {
    	logger.info("setting eureka instance status to DOWN");
    	applicationInfoManager.setInstanceStatus(InstanceInfo.InstanceStatus.DOWN);
    }
    
}
