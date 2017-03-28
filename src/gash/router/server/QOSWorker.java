package gash.router.server;

import gash.router.container.RoutingConf;
import gash.router.server.tasks.NoOpBalancer;
import gash.router.server.tasks.TaskList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import routing.Pipe.CommandMessage;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by henrywan16 on 3/25/17.
 */
public class QOSWorker implements QosInterface{
    protected static Logger logger = LoggerFactory.getLogger("qosworker");
    protected static QOSWorker instance;
    private boolean forever;
    private int threadLimit;
    private int queueLimit;
    protected RoutingConf conf;
    private LinkedBlockingDeque<CommandMessage> simpleQueue;

    public QOSWorker() {
        this.forever = true;
        this.threadLimit = 10;
        this.queueLimit = 100;
        this.simpleQueue = new LinkedBlockingDeque<CommandMessage>(this.queueLimit);
        instance = this;
    }

    public QOSWorker(RoutingConf conf) {
        this.forever = true;
        this.threadLimit = 10;
        this.queueLimit = 100;
        this.conf = conf;
        this.simpleQueue = new LinkedBlockingDeque<CommandMessage>(this.queueLimit);
        instance = this;
    }

    public static synchronized QOSWorker getInstance() {
        if (instance == null) {
            instance = new QOSWorker();
        }
        return instance;
    }

    public RoutingConf getConf() {
        return conf;
    }

    public void setConf(RoutingConf conf) {
        this.conf = conf;
    }

    public void shutdown() {
        forever = false;
    }

    public void handleMessage() {
        while (forever) {
            while (simpleQueue.isEmpty() == false) {
                CommandMessage commandMessage = simpleQueue.poll();
//                TransferMessage tm = new TransferMessage();
//                Thread subThread = new Thread(tm);
//                subThread.start();
            }
        }
    }

    /**
     * SimpleQueue dequeue
     * @return
     */
    @Override
    public CommandMessage dequeue() {
        if (this.simpleQueue.isEmpty() == false) {
            return this.simpleQueue.poll();
        }
        else {
            logger.error("Dequeue failed: simpleQueue is empty.");
            return null;
        }
    }

    /**
     * SimpleQueue enqueue
     * @param message
     */
    @Override
    public void enqueue(CommandMessage message) {
        if (this.simpleQueue.size() < this.queueLimit) {
            try {
                this.simpleQueue.put(message);
            } catch (InterruptedException e) {
                logger.error("Enqueue failed: null element or interrupt waitting.");
                e.printStackTrace();
            }
        }
    }

    private static class TransferMessage implements Runnable {
        private ServerState state;

        public TransferMessage(RoutingConf conf) {
            if (conf == null)
                throw new RuntimeException("missing conf");

            state = new ServerState();
            state.setConf(conf);

            TaskList tasks = new TaskList(new NoOpBalancer());
            state.setTasks(tasks);
        }

        @Override
        public void run() {
            try {
                // direct no queue
                // CommConnection.getInstance().write(rb.build());

                // using queue
                logger.info("MessageClient send CommandMessage with ping=true to Netty Channel! ");
//                CommConnection.getInstance().enqueue();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
