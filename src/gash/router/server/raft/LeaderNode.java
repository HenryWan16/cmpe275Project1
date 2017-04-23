package gash.router.server.raft;

import java.util.Hashtable;

import gash.router.server.edges.EdgeInfo;
import pipe.work.Work.WorkMessage;

public class LeaderNode implements NodeState {

	private RaftHandler handler;
	
	public LeaderNode(RaftHandler handler) {
		this.handler = handler;
	}
	
	public synchronized RaftHandler getHandler() {
		return this.handler;
	}

	@Override
	public synchronized void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public synchronized void run() {
		
		System.out.println("Node " + this.handler.getNodeId() + " - " + "IN LEADER MODE, term = " + this.handler.getTerm());
		try {
			if (this.handler.getNodeMode() == 3) {
		 		for (EdgeInfo ei:this.handler.getEdgeMonitor().getOutboundEdges().getMap().values()) {
		 			if (ei.isActive() && ei.getChannel().isActive()) {							
						//create hearbeat and send to follower
						ei.getChannel().writeAndFlush(MessageUtil.leaderSendHeartbeat(this.handler));
						System.out.println("Sending heartbeat to node "+ ei.getRef());
					}
				}
		 		//send heartbeat to all followers in cycle dt = 3sec
			}
	 		Thread.sleep(this.handler.getDt());
	 		return;
		 } catch(Exception e) {
			 e.printStackTrace();
		 }
	}

	@Override
	public synchronized void processHandleAVoteFromFollower(WorkMessage wm) {
		// TODO Auto-generated method stub
	}

	@Override
	public synchronized void processReplyAVoteToCandidate(WorkMessage wm) {
		// TODO Auto-generated method stub
	}

	@Override
	public synchronized void processReplyHeartBeatToLeader(WorkMessage wm) {
		// TODO Auto-generated method stub
	}

	@Override
	public synchronized void processSendUpdateLogs(WorkMessage wm) {
		// TODO Auto-generated method stub
		int sourceId = wm.getHeader().getNodeId();
		String fname = wm.getTaskStatus().getFilename();
		int chunkId =  wm.getTaskStatus().getChunkId();
		int chunkSize =  wm.getTaskStatus().getChunkSize();
		String host =  wm.getTaskStatus().getNode().getHost();
		int port =  wm.getTaskStatus().getNode().getPort();
		
		System.out.println("****************************");
		//add log into hashtable
		String key = fname + ";" + chunkId + ";" + chunkSize;
		String value = sourceId + ";" + host + ";" + port;
		
		if (handler.logs.containsKey(key)) { //replica node adding
			handler.logs.put(key, handler.logs.get(key) + value);
		} else {
			handler.logs.put(key, value);
		}
		
		//check if receive all task logs
		int counter = 0;
		Hashtable<String, String> newTable = new Hashtable<String, String>();
		for(String sKey: handler.logs.keySet()) {
			if (sKey.contains(fname)) {
				newTable.put(sKey, handler.logs.get(sKey));
				counter++;
			}
		}
		if (counter == chunkSize) {
			for (EdgeInfo ei:this.handler.getEdgeMonitor().getOutboundEdges().getMap().values()) {
	 			if (ei.isActive() && ei.getChannel().isActive()) {							
					//create hearbeat and send to follower
					ei.getChannel().writeAndFlush(MessageUtil.buildWMLogs(
								MessageUtil.buildHeader(handler.getNodeId(), System.currentTimeMillis()),
								newTable));
					System.out.println("Updating LOGS to follower "+ ei.getRef());
				}
			}
		}
	}
	
	@Override
	public synchronized void processSendRemoveLogs(WorkMessage wm) {
		String fname = wm.getTaskStatus().getFilename();
		for(String sKey: handler.logs.keySet()) {
			if (sKey.contains(fname)) {
				handler.logs.remove(sKey);
			}
		}
	}
	
	@Override
	public synchronized void processAddLogs(WorkMessage wm) {
		//not for leader
	}
}
