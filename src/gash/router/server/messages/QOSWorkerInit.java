//package gash.router.server.messages;
//
//import gash.router.server.ServerState;
//import io.netty.channel.ChannelInitializer;
//import io.netty.channel.ChannelPipeline;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
//import io.netty.handler.codec.LengthFieldPrepender;
//import io.netty.handler.codec.compression.ZlibCodecFactory;
//import io.netty.handler.codec.compression.ZlibWrapper;
//import io.netty.handler.codec.protobuf.ProtobufDecoder;
//import io.netty.handler.codec.protobuf.ProtobufEncoder;
//import pipe.work.Work;
//
///**
// * Created by henrywan16 on 4/3/17.
// */
//public class QOSWorkerInit extends ChannelInitializer<SocketChannel> {
//    boolean compress = false;
//    ServerState state;
//
//    public QOSWorkerInit(ServerState state, boolean enableCompression) {
//        super();
//        compress = enableCompression;
//        this.state = state;
//    }
//
//    @Override
//    public void initChannel(SocketChannel ch) throws Exception {
//        ChannelPipeline pipeline = ch.pipeline();
//
//        // Enable stream compression (you can remove these two if unnecessary)
//        if (compress) {
//            pipeline.addLast("deflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
//            pipeline.addLast("inflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
//        }
//
//        /**
//         * length (4 bytes).
//         *
//         * Note: max message size is 64 Mb = 67108864 bytes this defines a
//         * framer with a max of 64 Mb message, 4 bytes are the length, and strip
//         * 4 bytes
//         */
//        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));
//
//        // decoder must be first
//        pipeline.addLast("protobufDecoder", new ProtobufDecoder(Work.WorkMessage.getDefaultInstance()));
//        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
//        pipeline.addLast("protobufEncoder", new ProtobufEncoder());
//
//        // our server processor (new instance for each connection)
//        pipeline.addLast("handler", new QOSWorkHandler(state));
//    }
//}
