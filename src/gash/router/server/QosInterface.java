package gash.router.server;

import routing.Pipe.CommandMessage;

/**
 * Created by henrywan16 on 3/25/17.
 */
public interface QosInterface {
    public CommandMessage dequeue();
    public void enqueue(CommandMessage message);
}
