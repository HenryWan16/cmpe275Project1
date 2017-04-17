package gash.router.server.raft;

import java.util.Hashtable;

import gash.router.server.edges.EdgeInfo;
import pipe.work.Work.WorkMessage;

public class CandidateNode implements NodeState {

	private RaftHandler handler;
	private int numOfNodesActive = 1;
	private int numOfVote = 1; //vote for itself
	private boolean isAskedForVote = false;
	
	public CandidateNode(RaftHandler handler) {
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
		//System.out.println("IN CANDIDATE MODE");
		try {
			if (this.handler.getNodeMode() == 2) {
				//if timeout, cancel election and back to Follower
				if (this.handler.getTimeout() <= 0) {
					this.handler.setRandomTimeout();
					this.handler.decreaseTerm();
					System.out.println("Node " + this.handler.getNodeId() + " - " + "Timeout! Back to FOLLOWER");
					this.handler.setNodeState(this.handler.follower, 1);
					isAskedForVote = false;
//					this.handler.flushAllChannels();
					return;
				}
				
				//update total count for active nodes
				numOfNodesActive = 1;
				for (EdgeInfo ei:this.handler.getEdgeMonitor().getOutboundEdges().getMap().values()) {
					if (ei.isActive() && ei.getChannel().isActive()) {
						numOfNodesActive++;
					}
				}
				
				//nobody in the network, voted for itself to become leader
				if (numOfNodesActive == 1) {
					this.handler.setLeaderNodeId(this.handler.getNodeId());
					this.handler.setRandomTimeout();
					this.handler.increaseTerm();
					System.out.println("Node " + this.handler.getNodeId() + " - " +  "Become LEADER in term " + this.handler.getTerm());
					isAskedForVote = false;
					this.handler.setNodeState(this.handler.leader, 3);
//					this.handler.flushAllChannels();
					return;
					
				} else {
					//send a vote request to all follower
					if (!isAskedForVote) {
						System.out.println("Candidate node " + this.handler.getNodeId() + " is requesting vote to all followers");
						this.handler.increaseTerm();
						System.out.println("Active nodes = " + numOfNodesActive);
						numOfVote = 1;
						System.out.println("Candidate node " + this.handler.getNodeId() + " voted for itself");
						
						for (EdgeInfo ei:this.handler.getEdgeMonitor().getOutboundEdges().getMap().values()) {			
							if (ei.isActive() && ei.getChannel().isActive()) {		
								ei.getChannel().writeAndFlush(MessageUtil.candidateAskToVote(handler));
								System.out.println("Candidate is sending a vote request to node " + ei.getRef());
							}
						}
						isAskedForVote = true;
					}
				}
			}
			Thread.sleep(200);
			int dt = this.handler.getTimeout() - (int)(System.currentTimeMillis() - this.handler.getTimerStart());	
			this.handler.setTimeout(dt);
			return;
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	@Override
	public synchronized void processReplyAVoteToCandidate(WorkMessage wm) {
		// TODO Auto-generated method stub
	}

	@Override
	public synchronized void processHandleAVoteFromFollower(WorkMessage wm) {

		if (this.handler.getNodeMode() == 2) {
			System.out.println("Node " + this.handler.getNodeId() + " - " + "Get voted from node "+  wm.getAVote().getVoterID() + " voted for node");
			numOfVote++;
			
			System.out.println("Node " + this.handler.getNodeId() + " - " + "Current voted = " + numOfVote + "/" + numOfNodesActive + " active nodes, needs " + (1+(numOfNodesActive/2)) + " votes to become LEADER");
			if (numOfVote >= (numOfNodesActive / 2)) {
				this.handler.setRandomTimeout();
				System.out.println("Node " + this.handler.getNodeId() + " - " +  " become LEADER in term " + this.handler.getTerm());
				this.handler.setLeaderNodeId(this.handler.getNodeId());
				this.handler.setNodeState(this.handler.leader, 3);
//				this.handler.flushAllChannels();
				isAskedForVote = false;
			}
		}
	}

	@Override
	public synchronized void processReplyHeartBeatToLeader(WorkMessage wm) {

		if (this.handler.getNodeMode() == 2) {
			this.handler.setRandomTimeout();
			System.out.println("Node " + this.handler.getNodeId() + " - " + "Received hearbeat from the Leader: "+ wm.getLeader().getLeaderId());
			this.handler.setLastKnownBeat(System.currentTimeMillis());
			this.handler.setLeaderNodeId(wm.getLeader().getLeaderId());
			this.handler.setTerm(wm.getLeader().getLeaderTerm());
			this.handler.setNodeState(this.handler.follower, 1);
//			this.handler.flushAllChannels();
			isAskedForVote = false;
		}
	}

	@Override
	public void processSendUpdateLogs(WorkMessage wm) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public synchronized void processAddLogs(WorkMessage wm) {
		Hashtable<String, String> newTable = new Hashtable<String, String>();
		for(String sKey: newTable.keySet()) {
			handler.logs.put(sKey, newTable.get(sKey));
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

}
