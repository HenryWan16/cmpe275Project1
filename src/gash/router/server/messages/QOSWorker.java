package gash.router.server.messages;

import gash.router.server.MessageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by henrywan16 on 3/25/17.
 */
public class QOSWorker implements Runnable{
    protected static Logger logger = LoggerFactory.getLogger("qosworker");
    protected static QOSWorker instance;
    private boolean forever;
    private QueueInterface queue;

    public QOSWorker() {
        this.forever = true;

        // We use SimpleQueue here;
        this.queue = new SimpleQueue();
        instance = this;
        init();
    }

    public static QOSWorker getInstance() {
        if (instance == null) {
            instance = new QOSWorker();
        }
        return instance;
    }

    public void shutdown() {
        forever = false;
    }

    public void init() {
        Thread cthread = new Thread(this);
        cthread.start();
    }

    @Override
    public void run() {
        while (forever) {
            /*
            if (queue.isEmpty() == false) {
                logger.info("ThreadLimit is " + MessageServer.threadLimit + " now.");
                if (MessageServer.threadLimit < 10) {
                    Session session = queue.dequeue();
                    MessageServer.addThreadLimit();
                    Thread subThread = new Thread(session);
                    subThread.start();
                }
            }
            */
            logger.info("Queue Size: " + queue.size());
            try {
                Thread.sleep(3000);
            }catch(InterruptedException e){
                System.out.println(e.toString());
            }
        }
    }

    public QueueInterface getQueue() {
        return queue;
    }
}
