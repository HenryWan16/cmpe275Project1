package gash.router.app;

/**
 * Created by henrywan16 on 3/27/17.
 */
public class TCPDestination {
    private String host;
    private int port;
    private int id;

    public TCPDestination() {
        this.host = "localhost";
        this.port = 8080;
        this.id = 0;
    }

    public TCPDestination(String host, int port) {
        this.host = host;
        this.port = port;
        this.id = 0;
    }

    public TCPDestination(String host, int port, int id) {
        this.host = host;
        this.port = port;
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
