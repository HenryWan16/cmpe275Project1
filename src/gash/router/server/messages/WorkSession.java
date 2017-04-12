package gash.router.server.messages;

import gash.router.server.MessageServer;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.work.Work;
import pipe.work.Work.Heartbeat;
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

    // When the server receive the commandMessage, how to deal with it?
    @Override
    public void handleMessage() {
        // Channel channel = initChannel()
        // TODO How can you implement this without if-else statements?
        try {
            if (msg.hasBeat()) {
                Heartbeat hb = msg.getBeat();
                logger.info("heartbeat from " + msg.getHeader());
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
            MessageServer.minThreadLimit();
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

    public Channel initChannel(String host, int port) {
        ChannelFuture channel = null;
        EventLoopGroup group = new NioEventLoopGroup();
        if (channel == null) {
            try {
                WorkInit mi = new WorkInit(state, false);
                Bootstrap b = new Bootstrap();
                b.group(group).channel(NioSocketChannel.class).handler(mi);
                b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
                b.option(ChannelOption.TCP_NODELAY, true);
                b.option(ChannelOption.SO_KEEPALIVE, true);
                logger.info("Monitor" + host + "," + port);
                channel = b.connect(host, port).syncUninterruptibly();
            } catch (Exception e) {
                logger.info("channel create failed!");
            }
        }
        if (channel != null)
            return channel.channel();
        else
            throw new RuntimeException("can not establish channel to server");
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
