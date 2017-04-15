package gash.router.server.raft;

import java.net.Inet4Address;
import java.net.UnknownHostException;

import pipe.common.Common.Header;
import pipe.work.Work.RegisterNode;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.RequestVote;
import pipe.election.Election.Vote;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class MessageUtil {
	public static int secret = 1234;
	
	public static WorkMessage leaderSendHeartbeat(RaftHandler handler) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);
			
		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);
			
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(handler.getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());
			
		LeaderStatus.Builder lb = LeaderStatus.newBuilder();
		lb.setLeaderId(handler.getNodeId());
		lb.setLeaderTerm(handler.getTerm());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setLeader(lb);
		wb.setSecret(secret);
		
		return wb.build();
	}
	
	public static WorkMessage candidateAskToVote(RaftHandler handler) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(handler.getNodeId());
		hb.setDestination(-1);	
		
		RequestVote.Builder rvb= RequestVote.newBuilder();
		rvb.setCandidateID(handler.getNodeId());	
		rvb.setCurrentTerm(handler.getTerm());
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setReqAVote(rvb);
		wb.setSecret(secret);	
		
		return wb.build();
	}
	
	public static WorkMessage followerSendVote(int voterId, int candidateId) {
		Vote.Builder vb=Vote.newBuilder();		
		vb.setVoterID(voterId);
		vb.setCandidateID(candidateId);
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();	
		wb.setAVote(vb);
		wb.setSecret(secret);
		
		return wb.build();
	}
	
	public static WorkMessage registerANewNode(int nodeId, int port) throws UnknownHostException {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());
		
		RegisterNode.Builder rnb= RegisterNode.newBuilder();
		rnb.setHost(Inet4Address.getLocalHost().getHostAddress());
		rnb.setPort(port);
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);				
		wb.setANode(rnb);
		wb.setSecret(secret);
		
		return wb.build();
	}

}
