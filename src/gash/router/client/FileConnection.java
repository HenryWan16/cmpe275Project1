package gash.router.client;


import file.FileOuterClass;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import routing.Pipe;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by sam on 4/13/17.
 */
public class FileConnection {

    protected static Logger logger = LoggerFactory.getLogger("connect");
    protected static AtomicReference<FileConnection> instance = new AtomicReference<>();

    private String host;
    private int port;
    private ChannelFuture channel;
    private EventLoopGroup group;
    LinkedBlockingDeque<FileOuterClass.Request> outbound;
    private FileWorker worker;

    protected FileConnection(String host, int port) {
        this.host = host;
        this.port = port;

        init();
    }

    public static FileConnection initConnection(String host, int port) {
        instance.compareAndSet(null, new FileConnection(host, port));
        return instance.get();
    }

    public static FileConnection getInstance() {
        // TODO throw exception if not initialized!
        return instance.get();
    }

    protected Channel connect(){
        if(channel == null)
            init();
        if (channel != null && channel.isSuccess() && channel.channel().isWritable())
            return channel.channel();
        else
            throw new RuntimeException("Not able to establish connection to server");
    }

    public void addListener(FileListener listener) {
        FileHandler handler = connect().pipeline().get(FileHandler.class);
        if (handler != null)
            handler.addListener(listener);
    }

    public void enqueue(FileOuterClass.Request req) throws Exception {
        // enqueue message
//        logger.info("outbound enque command message: "+req.getMessage());
        outbound.put(req);
    }

    private void init(){
        outbound = new LinkedBlockingDeque<>();
        group = new NioEventLoopGroup();
        try{
            FileInit fi = new FileInit(null, false);
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(fi);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.option(ChannelOption.SO_KEEPALIVE, true);

            // Make the connection attempt.
            channel = b.connect(host, port).syncUninterruptibly();

            // want to monitor the connection to the server s.t. if we loose the
            // connection, we can try to re-establish it.
            FileConnection.ClientClosedListener ccl = new ClientClosedListener(this);
            channel.channel().closeFuture().addListener(ccl);
        }catch(Throwable ex){
            ex.printStackTrace();
        }
        worker = new FileWorker(this);
        worker.setDaemon(true);
        worker.start();
    }

    public static class ClientClosedListener implements ChannelFutureListener {
        FileConnection cc;

        public ClientClosedListener(FileConnection cc) {
            this.cc = cc;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            // we lost the connection or have shutdown.
            System.out.println("--> client lost connection to the server");
            System.out.flush();

            // @TODO if lost, try to re-establish the connection
        }
    }

    public boolean write(FileOuterClass.Request msg) {
        logger.info("write message"+msg.toString());
        if (msg == null)
            return false;
        else if (channel == null)
            throw new RuntimeException("missing channel");

        // TODO a queue is needed to prevent overloading of the socket
        // connection. For the demonstration, we don't need it
        ChannelFuture cf = connect().writeAndFlush(msg);
        if (cf.isDone() && !cf.isSuccess()) {
            logger.error("failed to send message to server");
            return false;
        }

        return true;
    }
}
