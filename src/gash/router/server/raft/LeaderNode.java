package gash.router.server.raft;


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

}
