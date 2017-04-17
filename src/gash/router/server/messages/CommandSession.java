package gash.router.server.messages;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.storage.MySQLStorage;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common;
import pipe.common.Common.Header;
import pipe.common.Common.Response;
import pipe.common.Common.TaskType;
import pipe.common.Common.WriteBody;
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

        try {
            // If the current nodeId equals msg destination, we can accept it.
            // Or we will transfer the message to new node.

            if (msg.hasPing()) {
                logger.info("QoSWorker: Server CommandHandler received ping message");
                logger.info("QoSWorker: ping from " + msg.getHeader().getNodeId());
                //return ping ack
                Header.Builder hb = Header.newBuilder();
                hb.setNodeId(conf.getNodeId());
                hb.setTime(System.currentTimeMillis());

                CommandMessage.Builder cm = CommandMessage.newBuilder();
                cm.setHeader(hb);
                cm.setPing(true);
                channel.writeAndFlush(cm); //respond back to client
                
            } else if (msg.hasRequest()) {
            	TaskType type = msg.getRequest().getRequestType();
            	if (type == TaskType.READFILE) {
            		
            		
            	} else if (type == TaskType.WRITEFILE) {
            		WriteBody wb = msg.getRequest().getRwb();
            		
            		String fname = wb.getFilename();
            		int chunkId = wb.getChunk().getChunkId();
            		byte[] data = wb.getChunk().getChunkData().toByteArray();
            		int numOfChunk = wb.getChunk().getChunkSize();
            		String fileId = "";
            		if (wb.hasFileId()) {
            			fileId = ((Long)wb.getFileId()).toString();
            		}
            			
            		boolean result = MySQLStorage.getInstance().insertRecordFileChunk(fname, chunkId, data, numOfChunk, fileId);
            		if (result) {
            			Header.Builder hb = Header.newBuilder();
                        hb.setNodeId(conf.getNodeId());
                        hb.setTime(System.currentTimeMillis());

                        CommandMessage.Builder cm = CommandMessage.newBuilder();
                        cm.setHeader(hb);
                        
                        Response.Builder rb = Response.newBuilder();
                        
                        channel.writeAndFlush(cm); 
            		}
            	} else if (type == TaskType.DELETEFILE) {
            		
            	} else { //UPDATEFILE
            		
            	}
            	
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
