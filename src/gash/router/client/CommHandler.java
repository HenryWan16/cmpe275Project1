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

import gash.router.container.RoutingConf;
import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import routing.Pipe.CommandMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A client-side netty pipeline send/receive.
 * 
 * Note a management client is (and should only be) a trusted, private client.
 * This is not intended for public use.
 * 
 * @author gash
 * 
 */
public class CommHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected boolean debug = false;
	protected ServerState state;
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	//private volatile Channel channel;

	public CommHandler(ServerState state) {
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
		}

		if (debug)
			PrintUtil.printCommand(msg);

		// TODO How can you implement this without if-else statements?
		try {
			logger.info("Server CommHandler received string message!");
			logger.info("msg.hasMessage() = " + msg.hasMessage());
			// If the current nodeId equals msg destination, we can accept it.
			// Or we will transfer the message to new node.
			if (msg.hasMessage()) {
				logger.info("string message from " + msg.getHeader().getNodeId());
				logger.info("state.getConf().getNodeId() = " + state.getConf().getNodeId());
				logger.info("msg.getHeader().getDestination()" + msg.getHeader().getDestination());
				if (state.getConf().getNodeId() == msg.getHeader().getDestination()) {
					System.out.println("CommHandler: the message arrived the node.");
					System.out.println("Message: " + msg.getMessage());
				}
				else {
					System.out.println("CommHandler: transfered by the node. " + state.getConf().getNodeId());
					for (RoutingConf.RoutingEntry r : state.getConf().getRouting()) {
						String newHost = r.getHost();
						int newPort = r.getPort();
						int newId = r.getId();
						MessageClient mc = new MessageClient(newHost, newPort);
						mc.sendMessage(msg.getMessage(), msg.getHeader().getNodeId(), msg.getHeader().getDestination());
					}
				}
			}
		} catch (Exception e) {
			// TODO add logging
			Common.Failure.Builder eb = Common.Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
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
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		System.out.println("--> got incoming message");
		System.out.println("--> listeners.size() = " + listeners.size());
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);

			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
//			handleMessage(msg, ctx.channel());
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
