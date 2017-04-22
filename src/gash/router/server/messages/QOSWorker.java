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
//    private EdgeMonitor emon;

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
    	logger.info("QOSWorker Thread Working : ");
        while (forever) {
        	if (!queue.isEmpty()) {
        		//do work in queue
        		logger.info("Task dequeue.");
        		Session task = queue.dequeue();
        		task.handleMessage();
        		
        	} else {//queue is empty, ask for work ** stealing work
        		
        	}
        	
//        	logger.info("Queue Size: " + queue.size());
//          try { Thread.sleep(2000); } catch(Exception e){ }
        }
    }

    public QueueInterface getQueue() {
        return queue;
    }
}
