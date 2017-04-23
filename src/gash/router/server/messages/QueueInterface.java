package gash.router.server.messages;

/**
 * Created by henrywan16 on 3/25/17.
 */
public interface QueueInterface {
    public Session dequeue();
    public Session peekLast();
    public void enqueue(Session message);
    public boolean isEmpty();
    public int size();
}
