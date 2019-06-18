package eu.nimble.service.delegate;

/**
 * ServiceEndpoint information.
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 04/06/2019.
 */
public class ServiceEndpoint {
    private String id;
    private  String ip;
    private int port;
    private String appName;
    private String frontendServiceUrl;

    public ServiceEndpoint(String id, String ip, int port, String appName) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.appName = appName;
    }
    
    public String getId() {
    	return this.id;
    }
    
    public String getIp() {
    	return this.ip;
    }
    
    public int getPort() {
    	return this.port;
    }
    
    public String getAppName() {
    	return this.appName;
    }
    
    public String getFrontendServiceUrl() {
    	return this.frontendServiceUrl;
    }
    
    public void setFrontendServiceUrl(String frontendServiceUrl) {
    	this.frontendServiceUrl = frontendServiceUrl;
    }
    
    @Override
    public String toString() {
    	return "Service Endpoint:\n\t"
    			+ "id: " + this.id + ",\n\t"
				+ "ip: " + this.ip + ",\n\t"
				+ "port: " + this.port + ",\n\t"
				+ "frontendServiceUrl: " + this.frontendServiceUrl + ",\n\t"
				+ "appName: " + this.appName + "\n";  
    }
}

