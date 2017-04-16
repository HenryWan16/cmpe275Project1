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
public class CommHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected boolean debug = false;
	protected ServerState state;
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	//private volatile Channel channel;
	MergeWorker mergeWorker;

	public CommHandler() {
	}
	
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
		}else if(msg.hasResponse()){
			System.out.println("IN HERE*******");
			if(msg.getResponse().getReadResponse().getChunkLocationList() == null){
				//second response from server
				Common.Chunk chunk = msg.getResponse().getReadResponse().getChunk();
				mergeWorker = MergeWorker.getMergeWorkerInstance();
				mergeWorker.upDateTable(chunk);
			}else{
				//first response from server
				int numChunks = msg.getResponse().getReadResponse().getNumOfChunks();
				mergeWorker = MergeWorker.getMergeWorkerInstance();
				Thread mergeThread = new Thread(mergeWorker);
				mergeThread.start();
				mergeWorker.setTotalNoOfChunks(numChunks);
				//ask directly from server nodes for file chunks
				List<Common.ChunkLocation> list = msg.getResponse().getReadResponse().getChunkLocationList();
				String fname = msg.getResponse().getReadResponse().getFilename();
				for(int i=0;i<list.size();i++){
					CommandMessage.Builder cmb = CommandMessage.newBuilder();
					Common.Request.Builder rb = Common.Request.newBuilder();
					Common.ReadBody.Builder rdb = Common.ReadBody.newBuilder();
					rdb.setFilename(fname);
					rdb.setChunkId(list.get(i).getChunkid());
					rb.setRrb(rdb);
					cmb.setRequest(rb);
					String host = list.get(i).getNode(0).getHost();
					int port = list.get(i).getNode(0).getPort();
					CommConnection.initConnection(host, port);
					try {
						CommConnection.getInstance().enqueue(cmb.build());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}


		}

		if (debug)
			PrintUtil.printCommand(msg);

		QOSWorker qos = QOSWorker.getInstance();
		logger.info("QOSWorker Thread Working on CommandSession: ");
		Session session = new CommandSession(this.state, msg);
		qos.getQueue().enqueue(session);
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
