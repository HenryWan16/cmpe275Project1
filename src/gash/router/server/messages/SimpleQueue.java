package gash.router.server.messages;

import java.util.concurrent.LinkedBlockingDeque;


public class SimpleQueue implements QueueInterface{
    private LinkedBlockingDeque<Session> simpleMsgQueue;

    public SimpleQueue() {
        this.simpleMsgQueue = new LinkedBlockingDeque<Session>();
    }

    @Override
    public Session dequeue() {
        return this.simpleMsgQueue.poll();
    }

    @Override
    public Session peekLast() {
        return this.simpleMsgQueue.peekLast();
    }

    @Override
    public void enqueue(Session message) {
        this.simpleMsgQueue.offer(message);
    }

    @Override
    public boolean isEmpty() {
        return this.simpleMsgQueue.isEmpty();
    }
    
    @Override
    public int size() {return this.simpleMsgQueue.size();}
}
