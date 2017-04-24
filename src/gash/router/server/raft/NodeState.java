package gash.router.server.raft;

import pipe.work.Work.WorkMessage;

public interface NodeState {
	
	public void init();
	public void run();

	public void processHandleAVoteFromFollower(WorkMessage wm);
	public void processReplyAVoteToCandidate(WorkMessage wm);
	public void processReplyHeartBeatToLeader(WorkMessage wm);
	
}
