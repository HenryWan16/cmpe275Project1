package gash.router.server.messages;

import gash.router.container.RoutingConf;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.raft.LogUtil;
import gash.router.server.raft.MessageUtil;
import gash.router.server.raft.RaftHandler;
import gash.router.server.storage.MySQLStorage;
import io.netty.channel.Channel;

import java.net.Inet4Address;
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
            	TaskType type = msg.getRequest().getRequestType();
            	if (type == TaskType.READFILE) {

            		//get a list of location from logs
            		String fname = msg.getRequest().getRrb().getFilename();

        			if (fname.equals("log.txt")) { //return all the list of file
        				Hashtable<String, String> location = RaftHandler.getInstance().logs;
        				//parse the hashtable to get the list
        				String list ="";
        				for(String sKey: location.keySet()) {
        					String sValue = location.get(sKey);
        					System.out.println(sKey + "****" + sValue);
        					String keyParts[] = sKey.split(";");
        					String valueParts[] = sValue.split(";");
        					list += keyParts[0] + "-chunkID:" + keyParts[1] + "-chunkSize:" + keyParts[2] + " at nodes:\n";
        					for (int i=0; i<valueParts.length; i=i+3) {
        						list += "\tNode " + valueParts[i] + ": " + valueParts[i+1] + "\n";
        					}
        				}
        				System.out.println(list);

        				CommandMessage cm = MessageUtil.buildCommandMessage(
            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
            					null,
            					null,
            					MessageUtil.buildResponse(TaskType.READFILE, fname, null , null, 
            								MessageUtil.buildReadResponseAllListFiles(-1, fname, list)));
            			channel.writeAndFlush(cm);
        				
        			} else {
            		
	            		Hashtable<Integer, String> location = LogUtil.getListNodesToReadFile(fname);            		
	
	            		
	            		if (location != null) {
	            			//return to client a list of locations
	            			CommandMessage cm = MessageUtil.buildCommandMessage(
	            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
	            					null,
	            					null,
	            					MessageUtil.buildResponse(TaskType.READFILE, fname, null , null, 
	            								MessageUtil.buildReadResponse(-1, fname, null, location.size(), 
	            										location, null)));
	            			channel.writeAndFlush(cm);
	            			
	            		} else {
	            			//return to client FAIL to read
	            			CommandMessage cm = MessageUtil.buildCommandMessage(
	            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
	            					null,
	            					null,
	            					MessageUtil.buildResponse(TaskType.READFILE, fname, ResponseStatus.Fail , null, null));
	                        
	                        channel.writeAndFlush(cm);
	            		}
        			}
            		
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

            		//write to logs
            		if (result) {
            			//send logs to leader

            			String host = Inet4Address.getLocalHost().getHostAddress();
            			WorkMessage wm = MessageUtil.buildWMTaskStatus(
            					MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
            					MessageUtil.buildTaskStatus(fname, chunkId, numOfChunk,
            							MessageUtil.buildRegisterNode(host, conf.getWorkPort())));

            			EdgeInfo leaderEdgeInfo = RaftHandler.getInstance().getEdgeMonitor().getOutboundEdges().getMap().get(RaftHandler.getInstance().getLeaderNodeId());
            			
            			//check if it is a leader of not
            			if (conf.getNodeId() == RaftHandler.getInstance().getLeaderNodeId()) {
            				RaftHandler.getInstance().getNodeState().processSendUpdateLogs(wm);
            			} else if (leaderEdgeInfo.getChannel() != null && leaderEdgeInfo.getChannel().isActive())
            				leaderEdgeInfo.getChannel().writeAndFlush(wm);
            			
            			//send success message back to client
                        CommandMessage cm = MessageUtil.buildCommandMessage(
                        		MessageUtil.buildHeader(conf.getNodeId(), System.currentTimeMillis()),
                        		null, 
                        		null,
                        		MessageUtil.buildResponse(TaskType.WRITEFILE, fname, ResponseStatus.Success, null, null));
                        channel.writeAndFlush(cm); 
            		}
            	} else if (type == TaskType.DELETEFILE) {
            		
            		ReadBody rb = msg.getRequest().getRrb();
            		String fname = rb.getFilename();
            		Hashtable<Integer, String> location = LogUtil.getListNodesToReadFile(fname);

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
            	} else { //UPDATEFILE: 
            		
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
