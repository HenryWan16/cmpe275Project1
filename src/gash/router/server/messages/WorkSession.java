package gash.router.server.messages;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;

/**
 * Created by henrywan16 on 4/3/17.
 */
public class WorkSession implements Session, Runnable{
    protected static Logger logger = LoggerFactory.getLogger("server");
    private ServerState state;
    private WorkMessage msg;
    private Channel channel;

    public WorkSession(ServerState state, WorkMessage msg) {
        this.state = state;
        this.msg = msg;
    }

    public WorkSession(ServerState state, WorkMessage msg, Channel channel) {
        this.state = state;
        this.msg = msg;
        this.channel = channel;
    }

    // When the server receive the WorkMessage, how to deal with it?
    @Override
    public void handleMessage() {
        // Channel channel = initChannel()
        // TODO How can you implement this without if-else statements?
        try {
            if (msg.hasBeat()) {
//                Heartbeat hb = msg.getBeat();
//                logger.info("heartbeat from " + msg.getHeader());
                PrintUtil.printWork(msg);
            } else if (msg.hasPing()) {
                logger.info("Server WorkHandler received ping message!");
                logger.info("ping from " + msg.getHeader().getNodeId());
                //            boolean p = msg.getPing();
                //            WorkMessage.Builder rb = WorkMessage.newBuilder();
                //            rb.setPing(true);
                //            channel.write(rb.build());
            } else if (msg.hasErr()) {
                Common.Failure err = msg.getErr();
                logger.error("failure from " + msg.getHeader().getNodeId());
                // PrintUtil.printFailure(err);
            } else if (msg.hasTask()) {
                Work.Task t = msg.getTask();
            } else if (msg.hasState()) {
                Work.WorkState s = msg.getState();
            }
//            logger.info("MessageServer.threadLimit = " + MessageServer.threadLimit);
//            MessageServer.minThreadLimit();
//            logger.info("MessageServer.threadLimit = " + MessageServer.threadLimit);
        }
        catch (Exception e) {
            // TODO add logging
            Common.Failure.Builder eb = Common.Failure.newBuilder();
            eb.setId(state.getConf().getNodeId());
            eb.setRefId(msg.getHeader().getNodeId());
            eb.setMessage(e.getMessage());
            WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
            rb.setErr(eb);
            channel.write(rb.build());
        }
        System.out.flush();
    }

    @Override
    public void run() {
        handleMessage();
    }

    public ServerState getState() {
        return state;
    }

    public void setState(ServerState state) {
        this.state = state;
    }

    public WorkMessage getMsg() {
        return msg;
    }

    public void setMsg(WorkMessage msg) {
        this.msg = msg;
    }
}
