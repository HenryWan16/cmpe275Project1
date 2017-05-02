package gash.router.server.raft;


import gash.router.server.edges.EdgeInfo;
import pipe.work.Work.WorkMessage;

public class FollowerNode implements NodeState {

	private RaftHandler handler;
	private boolean isSentAVote = false;
	
	public FollowerNode(RaftHandler handler) {
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
		//System.out.println("IN FOLLOWER MODE");

		try {
			if (this.handler.getNodeMode() == 1) {
				if (this.handler.getTimeout() <= 0) {
					this.handler.setRandomTimeout();
					if (this.handler.getLeaderNodeId() > 0) {
						//still connected with leader, reset timeout
						this.handler.setLeaderNodeId(-1);
						
					} else { //become candidate
						System.out.println("Node " + this.handler.getNodeId() + " - " + "No signal from leader, change to CANDIDATE state");
						this.handler.setNodeState(this.handler.candidate, 2);
					}
					return;
				}
			}
			int dt = this.handler.getTimeout() - (int)(System.currentTimeMillis() - this.handler.getTimerStart());	
			this.handler.setTimeout(dt);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void processHandleAVoteFromFollower(WorkMessage wm) {
		// TODO Auto-generated method stub
	}
	

	@Override
	public synchronized void processReplyAVoteToCandidate(WorkMessage wm) {
		System.out.println("received vote request");
		if (this.handler.getNodeMode() == 1) {
			//check if it already sent a vote to candidate on this term
	    	if (this.handler.getTerm() < wm.getReqAVote().getCurrentTerm() && !isSentAVote) {    			
				
	    		//send out a vote message
	    		int candidateNode = wm.getReqAVote().getCandidateID();
	    		int myNode = this.handler.getNodeId();
	    		System.out.println("Node " + this.handler.getNodeId() + " - " + "Voted for CANDIDATE node " + candidateNode + " in term "
							+ wm.getReqAVote().getCurrentTerm());
	
	    		EdgeInfo ei = this.handler.getEdgeMonitor().getOutboundEdges().getMap().get(candidateNode);
	    		if (ei.isActive() && ei.getChannel().isActive()) {
					System.out.println("Node " + this.handler.getNodeId() + " - " + "Voted for CANDIDATE node " + candidateNode + " in term "
							+ wm.getReqAVote().getCurrentTerm());
					ei.getChannel().writeAndFlush(MessageUtil.followerSendVote(myNode, candidateNode));
	    		}
	    		isSentAVote = true;
				this.handler.setRandomTimeout();
	    	}
		}
	}
	

	@Override
	public synchronized void processReplyHeartBeatToLeader(WorkMessage wm) {
		
		if (this.handler.getNodeMode() == 1) {
			this.handler.setRandomTimeout();	
			System.out.println("Node " + this.handler.getNodeId() + " - " + "Received hearbeat from the Leader: "+ wm.getLeader().getLeaderId());
			this.handler.setLastKnownBeat(System.currentTimeMillis());
			this.handler.setLeaderNodeId(wm.getLeader().getLeaderId());
			if (this.handler.getTerm() < wm.getLeader().getLeaderTerm()) {
				this.handler.setTerm(wm.getLeader().getLeaderTerm());
				isSentAVote = false; //reset
			}
		}
	}

}
