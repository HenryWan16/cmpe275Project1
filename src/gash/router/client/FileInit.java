package gash.router.client;

import gash.router.container.RoutingConf;
import gash.router.server.*;
import gash.router.server.FileHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import routing.Pipe;

/**
 * Created by sam on 4/13/17.
 */
public class FileInit extends ChannelInitializer<SocketChannel> {

    boolean compress = false;
    RoutingConf conf;

    public FileInit(RoutingConf conf, boolean enableCompression) {
        super();
        compress = enableCompression;
        this.conf = conf;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        if (compress) {
            pipeline.addLast("deflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
            pipeline.addLast("inflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
        }
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));

        // decoder must be first
        pipeline.addLast("protobufDecoder", new ProtobufDecoder(Pipe.CommandMessage.getDefaultInstance()));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("protobufEncoder", new ProtobufEncoder());


        // our server processor (new instance for each connection)
        pipeline.addLast("handler", new FileHandler());
    }
}
