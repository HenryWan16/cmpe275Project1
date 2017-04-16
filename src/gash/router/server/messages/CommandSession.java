package gash.router.server.messages;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.common.Common.Header;
import routing.Pipe.CommandMessage;

/**
 * Created by henrywan16 on 4/3/17.
 */
public class CommandSession implements Session, Runnable{
    protected static Logger logger = LoggerFactory.getLogger("server");
    //private ServerState state;
    private RoutingConf conf;
    private CommandMessage msg;
    private Channel channel;

    public CommandSession(RoutingConf conf, CommandMessage msg, Channel channel) {
        //this.state = state;
        this.conf = conf;
        this.msg = msg;
        this.channel = channel;
    }

    // When the server receive the commandMessage, how to deal with it?
    @Override
    public void handleMessage() {
//        if (msg.hasBeat()) {
//            Work.Heartbeat hb = msg.getBeat();
//            logger.debug("heartbeat from " + msg.getHeader().getNodeId());
//        } else if (msg.hasPing()) {
//            logger.info("Server WorkHandler received ping message!");
//            logger.info("ping from " + msg.getHeader().getNodeId());
//            boolean p = msg.getPing();
//            Work.WorkMessage.Builder rb = Work.WorkMessage.newBuilder();
//            rb.setPing(true);
//            channel.write(rb.build());
//        } else if (msg.hasErr()) {
//            Common.Failure err = msg.getErr();
//            logger.error("failure from " + msg.getHeader().getNodeId());
//            // PrintUtil.printFailure(err);
//        } else if (msg.hasTask()) {
//            Work.Task t = msg.getTask();
//        } else if (msg.hasState()) {
//            Work.WorkState s = msg.getState();
//        }
        // TODO How can you implement this without if-else statements?
        try {
            // If the current nodeId equals msg destination, we can accept it.
            // Or we will transfer the message to new node.
//            if (msg.hasMessage()) {
//                logger.info("string message from " + msg.getHeader().getNodeId());
//                logger.info("state.getConf().getNodeId() = " + state.getConf().getNodeId());
//                logger.info("msg.getHeader().getDestination()" + msg.getHeader().getDestination());
//                if (state.getConf().getNodeId() == msg.getHeader().getDestination()) {
//                    System.out.println("CommHandler: the message arrived the node.");
//                    System.out.println("Message: " + msg.getMessage());
//                }
//                else {
//                    System.out.println("CommHandler: transfered by the node. " + state.getConf().getNodeId());
//                    for (RoutingConf.RoutingEntry r : state.getConf().getRouting()) {
//                        String newHost = r.getHost();
//                        int newPort = r.getPort();
//                        int newId = r.getId();
//                        MessageClient mc = new MessageClient(newHost, newPort);
//                        mc.sendCommMessage(msg.getMessage(), msg.getHeader().getNodeId(), msg.getHeader().getDestination());
//                    }
//                }
//                MessageServer.minThreadLimit();
//            }
            if(msg.hasPing()){
                logger.info("QoSWorker: Server CommandHandler received ping message");
                logger.info("QoSWorker: ping from " + msg.getHeader().getNodeId());
                //return ping ack
                Header.Builder hb = Header.newBuilder();
                hb.setNodeId(conf.getNodeId());
                hb.setTime(System.currentTimeMillis());

                CommandMessage.Builder cm = CommandMessage.newBuilder();
                cm.setHeader(hb);
                cm.setPing(true);
                channel.writeAndFlush(cm);
            }

        } catch (Exception e) {
            // TODO add logging
            Common.Failure.Builder eb = Common.Failure.newBuilder();
            eb.setId(conf.getNodeId());
            eb.setRefId(msg.getHeader().getNodeId());
            eb.setMessage(e.getMessage());
            CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
            rb.setErr(eb);
            channel.write(rb.build());
        }
        System.out.flush();
    }

    @Override
    public void run() {
        handleMessage();
    }

    public CommandMessage getMsg() {
        return msg;
    }

    public void setMsg(CommandMessage msg) {
        this.msg = msg;
    }
}
