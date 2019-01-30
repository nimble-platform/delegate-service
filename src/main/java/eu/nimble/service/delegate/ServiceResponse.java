package eu.nimble.service.delegate;

/**
 * ServiceResponse information.
 *
 * Created by Idan Zach (idanz@il.ibm.com) 01/30/2019.
 */
public class ServiceResponse {
    public String id;
    public String data;
    public int statusCode;

    public ServiceResponse(String id, String data, int statusCode) {
        this.id = id;
        this.data = data;
        this.statusCode = statusCode;
    }
}

