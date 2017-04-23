package gash.router.client;

import file.FileOuterClass;
import gash.router.container.RoutingConf;
import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import gash.router.server.messages.CommandSession;
import gash.router.server.messages.QOSWorker;
import gash.router.server.messages.Session;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import routing.Pipe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by sam on 4/13/17.
 */
public class FileHandler extends SimpleChannelInboundHandler<FileOuterClass.Request> {

    protected static Logger logger = LoggerFactory.getLogger("connect");
    protected boolean debug = false;
    protected RoutingConf conf;
    protected ConcurrentMap<String, FileListener> listeners = new ConcurrentHashMap<String, FileListener>();
    //private volatile Channel channel;

    public FileHandler(RoutingConf conf) {
        this.conf = conf;
    }

    /**
     * Notification registration. Classes/Applications receiving information
     * will register their interest in receiving content.
     *
     * Note: Notification is serial, FIFO like. If multiple listeners are
     * present, the data (message) is passed to the listener as a mutable
     * object.
     *
     * @param listener
     */
    public void addListener(FileListener listener) {
        if (listener == null)
            return;

        listeners.putIfAbsent(listener.getListenerID(), listener);
    }


    /**
     * a message was received from the server. Here we dispatch the message to
     * the client's thread pool to minimize the time it takes to process other
     * messages.
     *
     * @param ctx
     *            The channel the message was received from
     * @param msg
     *            The message
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FileOuterClass.Request msg) throws Exception {
        System.out.println("--> got incoming message");
        for (String id : listeners.keySet()) {
            FileListener cl = listeners.get(id);

            // TODO this may need to be delegated to a thread pool to allow
            // async processing of replies
            cl.onMessage(msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        System.out.println("--> user event: " + evt.toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Unexpected exception from channel.", cause);
        ctx.close();
    }
}
