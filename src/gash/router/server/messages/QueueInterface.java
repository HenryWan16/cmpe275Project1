package gash.router.server.messages;


public interface QueueInterface {
    public Session dequeue();
    public Session peekLast();
    public void enqueue(Session message);
    public boolean isEmpty();
    public int size();
}
