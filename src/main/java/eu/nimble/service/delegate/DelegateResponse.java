package eu.nimble.service.delegate;

public class DelegateResponse {

    private int status;
    private String data;

    public DelegateResponse(int status, String data) {
        this.status = status;
        this.data = data;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
