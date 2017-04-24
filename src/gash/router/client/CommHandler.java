/*
 * copyright 2016, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.client;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import gash.router.server.messages.CommandSession;
import gash.router.server.messages.QOSWorker;
import gash.router.server.messages.Session;
import gash.router.server.raft.MessageUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common;
import pipe.common.Common.Response;
import pipe.common.Common.TaskType;
import routing.Pipe.CommandMessage;

/**
 * A client-side netty pipeline send/receive.
 * 
 * Note a management client is (and should only be) a trusted, private client.
 * This is not intended for public use.
 * 
 * @author gash
 * 
 */
// after the client received a response from the server, CommHandler will solve the response.
public class CommHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected boolean debug = false;
	protected ServerState state;
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	//private volatile Channel channel;
	MergeWorker mergeWorker;

	public CommHandler() {
     System.out.println("CommHandler Init");
	}
	
	public CommHandler(ServerState state) {
    System.out.println("CommHandler Init");
		this.state = state;
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
	public void addListener(CommListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
	}

	/**
	 * override this method to provide processing behavior. T
	 *
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;

		}else if(msg.hasPing()){
			//if the destination is this node's id then it is a returned ping
			if(msg.getHeader().getDestination() == -1) {
				//logger.info("Received returned ping from " + msg.getHeader().getNodeId());
				logger.info("Ping success");
			}else{
				Common.Header.Builder hb = Common.Header.newBuilder();
				//node id -1 is client
				hb.setNodeId(-1);
				hb.setDestination(msg.getHeader().getNodeId());
				hb.setTime(System.currentTimeMillis());
				CommandMessage.Builder rb = CommandMessage.newBuilder();
				rb.setHeader(hb);
				rb.setPing(true);
				channel.writeAndFlush(rb);
			}
		}else if(msg.hasResponse()){
			TaskType type = msg.getResponse().getResponseType();
			Response.Status status =  msg.getResponse().getStatus();
			
			if (type == TaskType.RESPONSEREADFILE) {
				
//				if (msg.getResponse().getFilename().equals("log.txt")) {
//					String result = msg.getResponse().getReadResponse().getFileExt();
//					
//					if (result.equals("")) result = "All the servers are empty!!";
//					System.out.print(result);
//					
//					return;
//				}
				
				if (status == Response.Status.FILENOTFOUND) {
					System.out.println("The file " + msg.getResponse().getFilename() + " is not in the servers.");
					return;
				} 
				//response has data
				else if(msg.getResponse().getReadResponse().hasChunk()){
					//second response from server
					logger.info("++++++++++++++++++ Begin to merge chunks +++++++++++++++++++++++++++++++++++++++");
					Common.Chunk chunk = msg.getResponse().getReadResponse().getChunk();
					MergeWorker.upDateTable(chunk);
					return;
					
				} else {
					//first response from server
					int numChunks = msg.getResponse().getReadResponse().getNumOfChunks();
					mergeWorker = MergeWorker.getMergeWorkerInstance();
					Thread mergeThread = new Thread(mergeWorker);
					mergeThread.start();
					mergeWorker.setTotalNoOfChunks(numChunks);
					
					// get the HashMap<chunkID, Location> from the readResponse.
					List<Common.ChunkLocation> list = msg.getResponse().getReadResponse().getChunkLocationList();
					String fname = msg.getResponse().getReadResponse().getFilename();
					// TODO if chunkID = n; we need to send n requests to the location
					int chunkSize = list.size();
					for(int i=0;i<chunkSize;i++){
						
						int chunkId = list.get(i).getChunkid();
						
						CommandMessage cm = MessageUtil.buildCommandMessage(
            					MessageUtil.buildHeader(999, System.currentTimeMillis()),
            					null,
            					MessageUtil.buildRequest(TaskType.REQUESTREADFILE, null, MessageUtil.buildReadBody(fname, -1, chunkId, chunkSize)),
            					null);
						logger.info("############SEND RESQUEST FOR EACH CHUNK###");
						try {
							CommConnection.getInstance().enqueue(cm);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			} else if (type == TaskType.RESPONSEWRITEFILE) {
				if (status == Response.Status.SUCCESS) {
					System.out.println("The file " + msg.getResponse().getFilename() + " has been successfully uploaded");	
				} else
					System.out.println("Failed to upload file " + msg.getResponse().getFilename() + " to the server.");
				
			} else {
				 //delete?
			}


		}

		if (debug) {
			PrintUtil.printCommand(msg);
		}
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
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		System.out.println("--> got incoming message");
		System.out.println("--> listeners.size() = " + listeners.size());
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);

			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}
		handleMessage(msg, ctx.channel());
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
