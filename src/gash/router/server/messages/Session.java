package gash.router.server.messages;


public interface Session extends Runnable{
    public void handleMessage();
}
