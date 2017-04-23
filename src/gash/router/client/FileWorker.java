package gash.router.client;

import file.FileOuterClass;
import io.netty.channel.Channel;
import routing.Pipe;

/**
 * Created by sam on 4/13/17.
 */
public class FileWorker extends Thread{
    private FileConnection conn;
    private boolean forever = true;

    public FileWorker(FileConnection conn){
        this.conn = conn;
        if(conn.outbound == null)
            throw new RuntimeException("File queu null");
    }

    @Override
    public void run() {
        System.out.println("Start file thread");
        System.out.flush();
        Channel ch = conn.connect();
        if (ch == null || !ch.isOpen() || !ch.isActive()) {
            CommConnection.logger.error("connection missing, no outbound communication");
            return;
        }

        while (true) {
            if (!forever && conn.outbound.size() == 0)
                break;

            try {
                // block until a message is enqueued AND the outgoing
                // channel is active
                FileOuterClass.Request msg = conn.outbound.take();
                if (ch.isWritable()) {
                    if (!conn.write(msg)) {
                        conn.outbound.putFirst(msg);
                    }

                    System.out.flush();
                } else {
                    System.out.println("--> channel not writable- tossing out msg!");

                    // conn.outbound.putFirst(msg);
                }

                System.out.flush();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                break;
            } catch (Exception e) {
                CommConnection.logger.error("Unexpected communcation failure", e);
                break;
            }
        }

        if (!forever) {
            CommConnection.logger.info("connection queue closing");
        }
    }
}
