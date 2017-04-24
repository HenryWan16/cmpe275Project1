package gash.router.server.messages;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.raft.LogUtil;
import gash.router.server.raft.MessageUtil;
import gash.router.server.raft.RaftHandler;
import gash.router.server.storage.ClassFileChunkRecord;
import gash.router.server.storage.MySQLStorage;
import io.netty.channel.Channel;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common;
import pipe.common.Common.Header;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Response;
import pipe.common.Common.TaskType;
import pipe.common.Common.WriteBody;
import pipe.work.Work.WorkMessage;
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
    	TaskType type = msg.getRequest().getRequestType();

        try {
            // If the current nodeId equals msg destination, we can accept it.
            // Or we will transfer the message to new node.

           	if (msg.hasPing()) {
                //return ping ack
                Header.Builder hb = Header.newBuilder();
                hb.setNodeId(conf.getNodeId());
                hb.setTime(System.currentTimeMillis());

                CommandMessage.Builder cm = CommandMessage.newBuilder();
                cm.setHeader(hb);
                cm.setPing(true);
                channel.writeAndFlush(cm); //respond back to client
                
            } else if (msg.hasRequest()) {
            	logger.info("CommendSession handleMessage RequestType is " + type);
            	// key = chunkID; 
    			// value = sourceId + ";" + host + ";" + port;
    			Hashtable<Integer, String> location = new Hashtable<Integer, String>();
        		String fname = msg.getRequest().getRrb().getFilename();
        		
        		/**************** READ ****************/
            	if (type == TaskType.REQUESTREADFILE) {
            		logger.info("read fileName is " + fname);
            		MySQLStorage mySQLStorage = MySQLStorage.getInstance();
            		
            		// If the fname exists on the server.
            		if (fname.equals("ls_all_the_files_and_chunks")) {
            			ArrayList<ClassFileChunkRecord> fileList = mySQLStorage.selectAllRecordsFileChunk();
                		//return to client a HashMap of locations
            			CommandMessage cm = MessageUtil.buildCommandMessage(
            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
            					null,
            					null,
            					MessageUtil.buildResponse(TaskType.RESPONSEREADFILE, fname, null , null, 
            								MessageUtil.buildReadResponseAllFiles(-1, fname, fileList.size(), 
            										fileList, null)));
                		// logger.info("READFILE location isn't null and " + cm.toString());
                		channel.writeAndFlush(cm);
                		
            		} else if (mySQLStorage.checkFileExist(fname)) {
            			// The first time to receive the message from the client. 
            			// The client says that "I want to read the file whose name is fname." chunkID in the request will be -1.
            			int chunkId = msg.getRequest().getRrb().getChunkId();
            			logger.info("chunkID is " + chunkId);
            			
            			// if we don't set chunkId in the CommandMessage, its default to be 0;
            			if (chunkId == -1) {
                			// Get all the chunkIDs of the file from the database on the leader. 
                			// All the nodes have the same data and we just return the records on the leader.
                			ArrayList<Integer> chunkIDArray = mySQLStorage.selectRecordFilenameChunkID(fname);
                    		logger.info("The first time to receive the read request from client, and server will return a location String");
                			String host = "";
    						try {
    							host = Inet4Address.getLocalHost().getHostAddress();
    						} catch (UnknownHostException e1) {
    							e1.printStackTrace();
    						}
                			int port = conf.getCommandPort();
                			int nodeID = conf.getNodeId();
                			String locationAddress = nodeID + ";" + host + ";" + port;
                			for (Integer e : chunkIDArray) {
                				location.put(e, locationAddress);
                			}

                    		//return to client a HashMap of locations
                			CommandMessage cm = MessageUtil.buildCommandMessage(
                					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
                					null,
                					null,
                					MessageUtil.buildResponse(TaskType.RESPONSEREADFILE, fname, null , null, 
                								MessageUtil.buildReadResponse(-1, fname, null, location.size(), 
                										location, null)));
                    		// logger.info("READFILE location isn't null and " + cm.toString());
                    		channel.writeAndFlush(cm);
                    	
                    	//ask for chunk data
                    	} else {
                    		// The second time to receive the message from the client which has received the HashMap<chunkID, location> from the server (Only one location of each chunk).
                    		// The client say that "I want to get the data of chunkId from the location."
                			mySQLStorage = MySQLStorage.getInstance();
                			String fileName = msg.getRequest().getRrb().getFilename();
                			int chunkID = msg.getRequest().getRrb().getChunkId();
                			ClassFileChunkRecord record = mySQLStorage.selectRecordFileChunk(fileName, chunkID);
                			byte[] chunkData = record.getData();
                			int chunkSize = record.getTotalNoOfChunks();
                			// logger.info("Send chunkData to the client: " + new String(chunkData));
                			CommandMessage cm = MessageUtil.buildCommandMessage(
                					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
                					null,
                					null,
                					MessageUtil.buildResponse(TaskType.RESPONSEREADFILE, fname, null , null, 
                								MessageUtil.buildReadResponse(-1, fname, null, location.size(), 
                										location, MessageUtil.buildChunk(chunkID, chunkData, chunkSize))));
                			// logger.info("Send chunkData to the client:" + cm.toString());
              
                			channel.writeAndFlush(cm);
                    	}
                	}
            		else {
            			// File isn't in the database and return to client FAIL to read
            			CommandMessage cm = MessageUtil.buildCommandMessage(
            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
            					null,
            					null,
            					MessageUtil.buildResponse(TaskType.RESPONSEREADFILE, fname, Response.Status.FILENOTFOUND , null, null));
                        logger.info("File " + fname + " isn't in the database. Read failed on the server.");
                        channel.writeAndFlush(cm);
                	}
            	}
            	
            	
            	/**************** WRITE ****************/
            	else if (type.equals(TaskType.REQUESTWRITEFILE)) {
    	        	logger.info("CommendSession type = " + type);
    	        	logger.info("type == TaskType.WRITEFILE is " + (type == TaskType.REQUESTWRITEFILE));
    	    		WriteBody wb = msg.getRequest().getRwb();
    	    		
    	    		fname = wb.getFilename();
    	    		int chunkId = wb.getChunk().getChunkId();
    	    		byte[] data = wb.getChunk().getChunkData().toByteArray();
    	    		int numOfChunk = wb.getChunk().getChunkSize();
    	    		String fileId = "";
    	    		if (wb.hasFileId()) {
    	    			fileId = ((Long)wb.getFileId()).toString();
    	    		}
    	    		
    	    		MySQLStorage mySQLStorage = MySQLStorage.getInstance();
            		
            		// If the file and chunkid already in server.
            		if (!mySQLStorage.checkFileChunkExist(fname, chunkId)) {
            			
	    	    		boolean result = mySQLStorage.insertRecordFileChunk(fname, chunkId, data, numOfChunk, fileId);
	    	    		logger.info("ChunkId " + chunkId);
	    	    		if (result) {
	    	    			// Replication
		    	    		for(EdgeInfo ei:RaftHandler.getInstance().getEdgeMonitor().getOutboundEdges().getMap().values()){
		    					if (ei.isActive() && ei.getChannel().isActive()) {
		    						int nodeId = ei.getRef();
		    						String nodehost = ei.getHost();
		    						int port = ei.getPort();
		    						
		    						WorkMessage wm = MessageUtil.replicateData(nodeId, nodehost, port, msg);
		    						ei.getChannel().writeAndFlush(wm);
		    					}
		    	    		}
	    	    		}
            		}
    	    	} else { }//update, delete:
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
            e.printStackTrace();
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
