//package gash.router.server.messages;
//
//import gash.router.server.PrintUtil;
//import gash.router.server.ServerState;
//import io.netty.channel.Channel;
//import io.netty.channel.ChannelHandlerContext;
//import io.netty.channel.SimpleChannelInboundHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import pipe.common.Common;
//import pipe.work.Work;
//
///**
// * Created by henrywan16 on 4/3/17.
// */
//public class QOSWorkHandler extends SimpleChannelInboundHandler<Work.WorkMessage> {
//    protected static Logger logger = LoggerFactory.getLogger("QOSWorker");
//    protected ServerState state;
//    protected boolean debug = false;
//
//    public QOSWorkHandler(ServerState state) {
//        if (state != null) {
//            this.state = state;
//        }
//    }
//
//    /**
//     * override this method to provide processing behavior. T
//     *
//     * @param msg
//     */
//    public void handleMessage(Work.WorkMessage msg, Channel channel) {
//        if (msg == null) {
//            // TODO add logging
//            System.out.println("ERROR: Unexpected content - " + msg);
//            return;
//        }
//
//        if (debug)
//            PrintUtil.printWork(msg);
//
//        // TODO How can you implement this without if-else statements?
//        try {
//            logger.info("QOSWorker getInstance()");
//            QOSWorker qos = QOSWorker.getInstance();
//            Session session = new WorkSession(this.state, msg);
//            qos.enqueue(session);
//
////            if (msg.hasBeat()) {
////                Work.Heartbeat hb = msg.getBeat();
////                logger.debug("heartbeat from " + msg.getHeader().getNodeId());
////            } else if (msg.hasPing()) {
////                logger.info("Server WorkHandler received ping message!");
////                logger.info("ping from " + msg.getHeader().getNodeId());
////                boolean p = msg.getPing();
////                Work.WorkMessage.Builder rb = Work.WorkMessage.newBuilder();
////                rb.setPing(true);
////                channel.write(rb.build());
////            } else if (msg.hasErr()) {
////                Common.Failure err = msg.getErr();
////                logger.error("failure from " + msg.getHeader().getNodeId());
////                // PrintUtil.printFailure(err);
////            } else if (msg.hasTask()) {
////                Work.Task t = msg.getTask();
////            } else if (msg.hasState()) {
////                Work.WorkState s = msg.getState();
////            }
//        } catch (Exception e) {
//            // TODO add logging
//            Common.Failure.Builder eb = Common.Failure.newBuilder();
//            eb.setId(state.getConf().getNodeId());
//            eb.setRefId(msg.getHeader().getNodeId());
//            eb.setMessage(e.getMessage());
//            Work.WorkMessage.Builder rb = Work.WorkMessage.newBuilder(msg);
//            rb.setErr(eb);
//            channel.write(rb.build());
//        }
//
//        System.out.flush();
//
//    }
//
//    /**
//     * a message was received from the server. Here we dispatch the message to
//     * the client's thread pool to minimize the time it takes to process other
//     * messages.
//     *
//     * @param ctx
//     *            The channel the message was received from
//     * @param msg
//     *            The message
//     */
//    @Override
//    protected void channelRead0(ChannelHandlerContext ctx, Work.WorkMessage msg) throws Exception {
//        handleMessage(msg, ctx.channel());
//    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        logger.error("Unexpected exception from downstream.", cause);
//        ctx.close();
//    }
//
//
//}
