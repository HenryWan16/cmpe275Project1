package gash.router.server.messages;

/**
 * Created by henrywan16 on 4/3/17.
 */
public interface Session extends Runnable{
    public void handleMessage();
}
