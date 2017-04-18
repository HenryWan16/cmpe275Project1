/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.messages.CommandSession;

import java.net.Inet4Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.messages.QOSWorker;
import gash.router.server.messages.Session;
import gash.router.server.messages.WorkSession;
import gash.router.server.raft.MessageUtil;
import gash.router.server.raft.RaftHandler;
import gash.router.server.storage.MySQLStorage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common;
import pipe.common.Common.Failure;
import pipe.common.Common.ResponseStatus;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import routing.Pipe;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ServerState state;
	protected boolean debug = false;

	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		}
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		
//		QOSWorker qos = QOSWorker.getInstance();
//		logger.info("QOSWorker Thread Working : ");
//		Session session = new WorkSession(this.state, msg);
//		qos.getQueue().enqueue(session);

		
		
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug)
			PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {
			if (msg.hasBeat()) {
				Heartbeat hb = msg.getBeat();
				logger.debug("heartbeat from " + msg.getHeader().getNodeId());
			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				channel.write(rb.build());
				
			} else if (msg.hasReqAVote()){				
				state.getHandler().getNodeState().processReplyAVoteToCandidate(msg);	        	
				
			} else if (msg.hasAVote()){
				state.getHandler().getNodeState().processHandleAVoteFromFollower(msg);		   
					
			} else if (msg.hasLeader()) {							
				state.getHandler().getNodeState().processReplyHeartBeatToLeader(msg);
				
			} else if (msg.hasANode()) {
				state.getHandler().getEdgeMonitor().createOutboundIfNew(msg.getHeader().getNodeId(), 
									msg.getANode().getHost(), msg.getANode().getPort());
				
			} else if (msg.hasTaskStatus()) {
				System.out.println("******hasTaskStatus******");
				state.getHandler().getNodeState().processSendUpdateLogs(msg);
				
			} else if (msg.getLogCount() > 0) {
				System.out.println("*****getLogCount*******");
				state.getHandler().getNodeState().processAddLogs(msg);
				
			} else if (msg.hasDeleteFile()) {
				System.out.println("*****hasDeleteFile*******");
				boolean result = MySQLStorage.getInstance().deleteRecordFileChunk(msg.getDeleteFile());
				
        		if (result) {
        			String host = Inet4Address.getLocalHost().getHostAddress();
        			WorkMessage wm = MessageUtil.buildWMDeleteLog(
        					MessageUtil.buildHeader(this.state.getConf().getNodeId(), System.currentTimeMillis()),
        					msg.getDeleteFile());
        			EdgeInfo leaderEdgeInfo = RaftHandler.getInstance().getEdgeMonitor().getOutboundEdges().getMap().get(RaftHandler.getInstance().getLeaderNodeId());
        			leaderEdgeInfo.getChannel().writeAndFlush(wm);
        		}
			} else if (msg.hasDeleteLog()) {
				System.out.println("****hasDeleteLog********");
				state.getHandler().getNodeState().processSendRemoveLogs(msg);
				
			} else if (msg.hasErr()) {
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
			    PrintUtil.printFailure(err);
			    
			} else if(msg.hasState()){
				//Other node is requesting work from this node
				System.out.println("received stealing request");
				if(!QOSWorker.getInstance().getQueue().isEmpty()){
					//No clue how to send the channel as a message
					//The node that steals the work from this node will not be able to talk to the client
					Pipe.CommandMessage cMsg = ((CommandSession)QOSWorker.getInstance().getQueue().dequeue()).getMsg();
	
					Common.Header.Builder hd = Common.Header.newBuilder();
					hd.setNodeId(state.getConf().getNodeId());
					hd.setTime(System.currentTimeMillis());
	
					WorkMessage.Builder wm = WorkMessage.newBuilder();
	
					wm.setHeader(hd);
					wm.setCmdMessage(cMsg);
					wm.setSecret(1234);
					channel.writeAndFlush(wm);
				}
			} else if (msg.hasCmdMessage()){
				Pipe.CommandMessage cmdMessage = msg.getCmdMessage();
				Session session1 = new CommandSession(state.getConf(), cmdMessage, channel);
				QOSWorker.getInstance().getQueue().enqueue(session1);
				
			} else if (msg.hasTask()) {
				Task t = msg.getTask();
			}
		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();

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
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		logger.info("WorkHandler channelRead0...");
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}