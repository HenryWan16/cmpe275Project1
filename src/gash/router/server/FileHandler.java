package gash.router.server;

import file.FileOuterClass;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sam on 4/14/17.
 */
public class FileHandler extends SimpleChannelInboundHandler<FileOuterClass.Request> {

    protected static Logger logger = LoggerFactory.getLogger("cmd");

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FileOuterClass.Request request) throws Exception {
        logger.info("file received");
    }
}
