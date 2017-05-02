package gash.router.server.messages;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.work.Work.WorkMessage;


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

        try {
            if (msg.hasBeat()) {
                PrintUtil.printWork(msg);
                
            } else if (msg.hasPing()) {
                logger.info("Server WorkHandler received ping message!");
                logger.info("ping from " + msg.getHeader().getNodeId());

            } else if (msg.hasErr()) {
                Common.Failure err = msg.getErr();
                logger.error("failure from " + msg.getHeader().getNodeId());
                PrintUtil.printFailure(err);
            
            } else if (msg.hasTask()) {

            	//Work.Task t = msg.getTask();
            	
            } else if (msg.hasState()) {

            	//Work.WorkState s = msg.getState();
            }
        }
        catch (Exception e) {
            // TODO add logging
            Common.Failure.Builder eb = Common.Failure.newBuilder();
            eb.setId(state.getConf().getNodeId());
            eb.setRefId(msg.getHeader().getNodeId());
            eb.setMessage(e.getMessage());
            WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
            rb.setErr(eb);
            rb.setSecret(1234);
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
