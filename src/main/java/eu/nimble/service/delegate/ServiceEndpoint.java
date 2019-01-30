package eu.nimble.service.delegate;

/**
 * ServiceEndpoint information.
 *
 * Created by Idan Zach (idanz@il.ibm.com) 01/30/2019.
 */
public class ServiceEndpoint {
    public String id;
    public String hostName;
    public int port;

    public ServiceEndpoint(String id, String hostName, int port) {
        this.id = id;
        this.hostName = hostName;
        this.port = port;
    }
}

