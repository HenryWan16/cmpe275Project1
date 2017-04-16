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
package gash.router.server.edges;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.router.server.messages.QOSWorker;
import gash.router.server.raft.MessageUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common;
import pipe.work.Work;


public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private EdgeList outboundEdges;
//	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private boolean forever = true;
	private boolean isStarted = false;
	

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
//		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

//	public void createInboundIfNew(int ref, String host, int port) {
//		inboundEdges.createIfNew(ref, host, port);
//	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}
	
	public EdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {
				// check if node gets initialized yet
				if (!isStarted) {
					for (EdgeInfo ei:this.outboundEdges.map.values()) {
						logger.info("Init the node itself and register to other\n");
						//update the its outboundEdges with neighboor, add missing ones
						if (ei.isActive() && ei.getChannel().isActive()) {
							int nodeId = ei.getRef();
							String host = ei.getHost();
							int port = ei.getPort();

							ei.getChannel().writeAndFlush(MessageUtil.registerANewNode(nodeId, host, port));
						}				
					 }
					isStarted = true;
				}
				
				//Check all neighbor nodes to get connected
				for(EdgeInfo ei:this.outboundEdges.map.values()) {
					if (ei.getChannel() == null || !ei.getChannel().isActive()) {
						//logger.info("trying to connect to node " + ei.getRef());
						try {
							Channel channel = createChannel(ei.getHost(), ei.getPort());

	                        if (channel != null && channel.isActive()) {
	                        	ei.setChannel(channel);                        	
	                            ei.setActive(true);                                
	                            logger.info("connected to node " + ei.getRef());
	                        } else {
	                        	
	                        }
						} catch (Exception e) { /*do not show anything */ }
					} 
					if(ei.getChannel() != null) {
						//check all edges for work is this node's queue is empty
						if (QOSWorker.getInstance().getQueue().isEmpty()) {
							Common.Header.Builder hb = Common.Header.newBuilder();
							hb.setNodeId(state.getConf().getNodeId());
							hb.setTime(System.currentTimeMillis());

							Work.WorkState.Builder ws = Work.WorkState.newBuilder();
							ws.setEnqueued(0);
							ws.setProcessed(1);

							Work.WorkMessage.Builder wm = Work.WorkMessage.newBuilder();
							wm.setHeader(hb);
							wm.setState(ws);
							wm.setSecret(100l);
							ei.getChannel().writeAndFlush(wm.build());
						}
					}


				}
				
				Thread.sleep(dt);
			} catch (InterruptedException e) {			
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}
	
	private Channel createChannel(String host, int port) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup eventLoop = new NioEventLoopGroup();
		WorkInit workInit = new WorkInit(state, false);
		
		try {
			b.group(eventLoop).channel(NioSocketChannel.class).handler(workInit);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000); //timeout in 10sec
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
    	} catch (Exception e) {
    		return null;
    	}
		
		return b.connect(host, port).syncUninterruptibly().channel();
	}
	
	

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}
}
