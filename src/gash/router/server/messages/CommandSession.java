package gash.router.server.messages;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.raft.LogUtil;
import gash.router.server.raft.MessageUtil;
import gash.router.server.raft.RaftHandler;
import gash.router.server.storage.ClassChunkIdFilename;
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
import pipe.common.Common.ResponseStatus;
import pipe.common.Common.TaskType;
import pipe.common.Common.TaskType.*;
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
        	if (type == TaskType.READFILE) {
        		logger.info("read fileName is " + fname);
        		MySQLStorage mySQLStorage = MySQLStorage.getInstance();
        		
        		// If the fname exists on the server.
        		if (mySQLStorage.checkFileExist(fname)) {
        			// The first time to receive the message from the client. 
        			// The client says that "I want to read the file whose name is fname." chunkID in the request will be -1.
        			int chunkId = msg.getRequest().getRrb().getChunkId();
        			logger.info("chunkID is " + chunkId);
        			// if we don't set chunkId in the CommandMessage, its default to be 0;
        			if (chunkId == 0) {
            			// Get all the chunkIDs of the file from the database on the leader. 
            			// All the nodes have the same data and we just return the records on the leader.
            			ArrayList<Integer> chunkIDArray = mySQLStorage.selectRecordFilenameChunkID(fname);
                		logger.info("The first time to receive the read request from client, and server will return a location String");
            			String host = "";
						try {
							host = Inet4Address.getLocalHost().getHostAddress();
						} catch (UnknownHostException e1) {
							// TODO Auto-generated catch block
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
            					MessageUtil.buildResponse(TaskType.READFILE, fname, null , null, 
            								MessageUtil.buildReadResponse(-1, fname, null, location.size(), 
            										location, null)));
                		logger.info("READFILE location isn't null and " + cm.toString());
                		channel.writeAndFlush(cm);
                			
                	} else {
                		// The second time to receive the message from the client which has received the HashMap<chunkID, location> from the server (Only one location of each chunk).
                		// The client say that "I want to get the data of chunkId from the location."
            			mySQLStorage = MySQLStorage.getInstance();
            			String fileName = msg.getRequest().getRrb().getFilename();
            			int chunkID = msg.getRequest().getRrb().getChunkId();
            			ClassFileChunkRecord record = mySQLStorage.selectRecordFileChunk(fileName, chunkID);
            			byte[] chunkData = record.getData();
            			int chunkSize = record.getTotalNoOfChunks();
            			logger.info("Send chunkData to the client: " + new String(chunkData));
            			CommandMessage cm = MessageUtil.buildCommandMessage(
            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
            					null,
            					null,
            					MessageUtil.buildResponse(TaskType.READFILE, fname, null , null, 
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
        					MessageUtil.buildResponse(TaskType.READFILE, fname, ResponseStatus.Fail , null, null));
                    logger.info("File " + fname + " isn't in the database. Read failed on the server.");
                    channel.writeAndFlush(cm);
            	}
        	}
        	else if (type.equals(TaskType.WRITEFILE)) {
	        	logger.info("CommendSession type = " + type);
	        	logger.info("type == TaskType.WRITEFILE is " + (type == TaskType.WRITEFILE));
	    		WriteBody wb = msg.getRequest().getRwb();
	    		
	    		fname = wb.getFilename();
	    		int chunkId = wb.getChunk().getChunkId();
	    		byte[] data = wb.getChunk().getChunkData().toByteArray();
	    		int numOfChunk = wb.getChunk().getChunkSize();
	    		String fileId = "";
	    		if (wb.hasFileId()) {
	    			fileId = ((Long)wb.getFileId()).toString();
	    		}
	    		
	    		logger.info("Going to Write " + new String(data));
	    		boolean result = MySQLStorage.getInstance().insertRecordFileChunk(fname, chunkId, data, numOfChunk, fileId);

//            		//write to logs
//            		if (result) {
//            			//send logs to leader
//
//            			String host = "";
//						try {
//							host = Inet4Address.getLocalHost().getHostAddress();
//						} catch (UnknownHostException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//            			WorkMessage wm = MessageUtil.buildWMTaskStatus(
//            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
//            					MessageUtil.buildTaskStatus(fname, chunkId, numOfChunk,
//            							MessageUtil.buildRegisterNode(host, conf.getWorkPort())));
//
//            			EdgeInfo leaderEdgeInfo = RaftHandler.getInstance().getEdgeMonitor().getOutboundEdges().getMap().get(RaftHandler.getInstance().getLeaderNodeId());
//            			
//            			//check if it is a leader of not
//            			if (conf.getNodeId() == RaftHandler.getInstance().getLeaderNodeId()) {
//            				RaftHandler.getInstance().getNodeState().processSendUpdateLogs(wm);
//            			} else if (leaderEdgeInfo.getChannel() != null && leaderEdgeInfo.getChannel().isActive())
//            				leaderEdgeInfo.getChannel().writeAndFlush(wm);
//            			
//            			//send success message back to client
//                        CommandMessage cm = MessageUtil.buildCommandMessage(
//                        		MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
//                        		null, 
//                        		null,
//                        		MessageUtil.buildResponse(TaskType.WRITEFILE, fname, ResponseStatus.Success, null, null));
//                        channel.writeAndFlush(cm); 
//            		}
        	}
	    	else if (type == TaskType.DELETEFILE) {	
	    		ReadBody rb = msg.getRequest().getRrb();
	    		fname = rb.getFilename();
	    		// Hashtable<Integer, String> location = LogUtil.getListNodesToReadFile(fname);

	    		//send to all nodes to delete the file
	    		boolean result = false;
	    		if (location != null) {
	    			for(Integer sKey: location.keySet()) {
	    				String list = location.get(sKey);
	    				String[] parts = list.split(";");
	    				
	    				for(int j=0; j<parts.length; j=j+3) {
	    					int nodeId = Integer.parseInt(parts[j]);
	//            					String host = parts[j+1];
	//            					int port = Integer.parseInt(parts[j+2]);
	    					
	    					//send WM to remove file and logs
	    					WorkMessage wm = MessageUtil.buildWMDeleteFile(
	            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()), fname);
	    					if (conf.getNodeId() != nodeId) {
	                			EdgeInfo node = RaftHandler.getInstance().getEdgeMonitor().getOutboundEdges().getMap().get(nodeId);
	                			if (node.getChannel() != null && node.getChannel().isActive())
	                				node.getChannel().writeAndFlush(wm);
	    					} else { //the node is in itself
	    						MySQLStorage.getInstance().deleteRecordFileChunk(fname);
	    						RaftHandler.getInstance().getNodeState().processSendRemoveLogs(wm);
	    					}
	    				}
	    			}
	        		result = true;
	    		}
    		
	    		ResponseStatus status;
	    		if (result) {
	    			status = ResponseStatus.Success;
	    		} else status = ResponseStatus.Fail;
	        		
	    		//send back to client
	    		CommandMessage cm = MessageUtil.buildCommandMessage(
						MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
						null,
						null,
						MessageUtil.buildResponse(TaskType.DELETEFILE, fname, status , null, null));
	            channel.writeAndFlush(cm);
	    	} else { 
	    		// UPDATEFILE: 
	    	}
        }
   		System.out.flush();
    }
    
//        } catch (Exception e) {
            // TODO add logging
//            Common.Failure.Builder eb = Common.Failure.newBuilder();
//            eb.setId(conf.getNodeId());
//            eb.setRefId(msg.getHeader().getNodeId());
//            eb.setMessage(e.getMessage());
//            CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
//            rb.setErr(eb);
//            channel.write(rb.build());
//            e.printStackTrace();
//        }

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
