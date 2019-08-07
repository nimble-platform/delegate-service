package eu.nimble.service.delegate.catalog;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Catalog Service Handler
 *
 * Created by Nir Rozenbaum (nirro@il.ibm.com) 08/05/2019.
 */
public class CatalogHandler {
	private static Logger logger = LogManager.getLogger(CatalogHandler.class);
	
    private static String SERVICE_URL = "CATALOG_SERVICE_BASE_URL";
    private static String SERVICE_PORT = "CATALOG_SERVICE_PORT";
	
    public static String GET_BINARY_CONTENTS_PATH = "/binary-contents";
    public static String GET_BINARY_CONTENTS_LOCAL_PATH = "/binary-contents/local";
    
    public String BaseUrl;
    public int Port;
    public String PathPrefix;
	
	public CatalogHandler() {
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
		
		logger.info("Service Handler is being initialized with base url = " + BaseUrl + ", path prefix = " + PathPrefix + ", port = " + Port + "...");
	}
}
