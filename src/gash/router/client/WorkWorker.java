package gash.router.client;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;


public class WorkWorker extends Thread{
    private WorkConnection conn;
    private boolean forever = true;

    public WorkWorker(WorkConnection conn) {
        this.conn = conn;

        if (conn.outbound == null)
            throw new RuntimeException("connection worker detected null queue");
    }

    @Override
    public void run() {
        System.out.println("--> starting WorkWorker thread");
        System.out.flush();

        Channel ch = conn.connect();
        if (ch == null || !ch.isOpen() || !ch.isActive()) {
            WorkConnection.logger.error("connection missing, no outbound communication");
            return;
        }

        while (true) {
            if (!forever && conn.outbound.size() == 0)
                break;

            try {
                // block until a message is enqueued AND the outgoing
                // channel is active
                WorkMessage msg = conn.outbound.take();
                System.out.println("--> Channel: WorkWorker is going to write message. ");
                if (ch.isWritable()) {
                    if (!conn.write(msg)) {
                        conn.outbound.putFirst(msg);
                    }

                    System.out.flush();
                } else {
                    System.out.println("--> channel not writable- tossing out msg!");

                }

                System.out.flush();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                break;
            } catch (Exception e) {
                WorkConnection.logger.error("Unexpected communcation failure", e);
                break;
            }
        }

        if (!forever) {
            WorkConnection.logger.info("connection queue closing");
        }
    }
}
