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

import gash.router.server.messages.CommandSession;
import gash.router.server.messages.QOSWorker;
import gash.router.server.messages.Session;

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import routing.Pipe.CommandMessage;
import pipe.common.Common.Header;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	QOSWorker qos;
	public CommandHandler(RoutingConf conf) {
		if (conf != null) {
			this.conf = conf;
		}

	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		try {
			// TODO How can you implement this without if-else statements?
			if (msg.hasPing()) {
				logger.info("Received a PING message.");
				logger.info("Node ID = " + msg.getHeader().getNodeId());
				logger.info("Destination ID = " + msg.getHeader().getDestination());
				int nodeId = msg.getHeader().getNodeId();
					//add into channels table
				if (nodeId > 10) {
					//save client channel
					if ((msg.getHeader().getNodeId() % 10) == RoutingConf.clusterId) {
						handleClientRequest(channel, nodeId);
					}
					
					if (msg.getHeader().getDestination() != RoutingConf.clusterId) {
						logger.info("Not a cluster destination, just forwarding the message.");
						if (ServerState.nextCluster.isActive()) {
							ServerState.nextCluster.writeAndFlush(msg);
						}
					} else {
						logger.info("Reached cluster destination, rebuiding & forwarding the message.");
						forwardMessage(msg, channel, nodeId);
					}
					
				} else { //node <10
					if ((msg.getHeader().getDestination() % 10) != RoutingConf.clusterId) {
						logger.info("Not a cluster destination, just forwarding the message.");
						if (ServerState.nextCluster.isActive()) {
							ServerState.nextCluster.writeAndFlush(msg);
						}
					} else {
						//stop here
						logger.info("REPLY BACK TO CLIENT HERE.");
						if (!ServerState.channelsTable.isEmpty()) {
							Hashtable<Channel, Integer> client = ServerState.channelsTable.get(msg.getHeader().getDestination());
							Channel clientChannel = client.keys().nextElement();
							clientChannel.writeAndFlush(msg);
							
							updateChannelsTable(client, clientChannel, nodeId);
						}
					}
				}
					

			} else if (msg.hasRequest()) {
				qos = QOSWorker.getInstance();
				Session session = new CommandSession(conf, msg, channel);
				qos.getQueue().enqueue(session);

			}

		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();
		
	}
	
	public static void handleClientRequest(Channel channel, int nodeId) {
		Hashtable<Channel, Integer> client = new Hashtable<Channel, Integer>();
		if (!ServerState.channelsTable.containsKey(nodeId)) {
			logger.info("Saving client channel");
			//first time here
			client.put(channel, 1);
			ServerState.channelsTable.put(nodeId, client);
			logger.info("table size: " + String.valueOf(ServerState.channelsTable.size()));
		} else { //update request count
			Hashtable<Channel, Integer> savedClient = ServerState.channelsTable.get(nodeId);
			Channel firstChannel = savedClient.keys().nextElement();
			int count = savedClient.get(firstChannel);
			savedClient.put(firstChannel, count+1);
			ServerState.channelsTable.put(nodeId, savedClient);
		}
	}
	
	public static void updateChannelsTable(Hashtable<Channel, Integer> client, Channel clientChannel, int nodeId) {
		int count = client.get(clientChannel);
		if (count == 1) {
			ServerState.channelsTable.remove(nodeId);
		} else { //minus request -1
			client.put(clientChannel, count-1);
			ServerState.channelsTable.put(nodeId, client);
		}
	}
	
	public void forwardMessage(CommandMessage msg, Channel channel, int nodeId) {
		logger.info("Forwarding message...");
		
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(msg.getHeader().getDestination());
		hb.setTime(msg.getHeader().getTime());
		hb.setDestination(msg.getHeader().getNodeId());
		hb.setMaxHops(msg.getHeader().getMaxHops() - 1);

		CommandMessage.Builder cmb = CommandMessage.newBuilder();
		cmb.setHeader(hb);
		cmb.setPing(true);
	
		if (ServerState.nextCluster.isActive()) {
			ServerState.nextCluster.writeAndFlush(cmb.build());
		} else {
			logger.info("Cannot connect to next cluster");
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
		// logger.info("CommandHandler Accept the message: " + acceptInboundMessage(msg));
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}