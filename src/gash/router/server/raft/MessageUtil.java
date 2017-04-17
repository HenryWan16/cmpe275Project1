package gash.router.server.raft;

import com.google.protobuf.ByteString;
import pipe.common.Common.*;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.RequestVote;
import pipe.election.Election.Vote;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.RegisterNode;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import routing.Pipe.CommandMessage;

import java.net.UnknownHostException;

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
	
	public static WorkMessage registerANewNode(int nodeId, String host, int port) throws UnknownHostException {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());
		
		RegisterNode.Builder rnb= RegisterNode.newBuilder();
		rnb.setHost(host);
		rnb.setPort(port);
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);				
		wb.setANode(rnb);
		wb.setSecret(secret);
		
		return wb.build();
	}

	/************ COMMAND MESSAGES ********/
		
	public static Chunk.Builder buildChunk(int id, byte[] data, int size) {
		Chunk.Builder chunk = Chunk.newBuilder();
		chunk.setChunkId(id);
		chunk.setChunkData(ByteString.copyFrom(data));
		chunk.setChunkSize(size);
		return chunk; 
	}
	
	public static Node.Builder buildNode(int id, String host, int port) {
		Node.Builder node = Node.newBuilder();
		node.setNodeId(id);
		node.setHost(host);
		node.setPort(port);
		return node;
	}
	
	public static ChunkLocation.Builder buildChunkLocation(int id, Node node) {
		ChunkLocation.Builder location = ChunkLocation.newBuilder();
		location.setChunkid(id);
		location.addNode(node);
		return location;
	}
	
	public static ReadResponse.Builder buildReadResponse(int fileId, String name, String ext, int noChunks, 
			ChunkLocation.Builder location, Chunk.Builder chunk) {
		ReadResponse.Builder rr = ReadResponse.newBuilder();
		rr.setFileId(fileId);
		rr.setFilename(name);
		if (ext != null) rr.setFileExt(ext);
		if (location != null) rr.addChunkLocation(location);
		if (chunk != null) rr.setChunk(chunk);
		return rr;
	}
	
	public static WriteResponse.Builder buildWriteResponse(int chunkId) {
		WriteResponse.Builder wr = WriteResponse.newBuilder();
		wr.addChunkId(chunkId);
		return wr;
	}
	
	public static Response.Builder buildResponse(TaskType task, String fname, ResponseStatus ack, WriteResponse.Builder wr, ReadResponse.Builder rr) {
		Response.Builder r = Response.newBuilder();
		r.setResponseType(task);
		if (fname != null) r.setFilename(fname);
		if (ack != null) r.setAck(ack);
		if (wr != null) r.setWriteResponse(wr);
		if (rr != null) r.setReadResponse(rr);
		return r;
	}
	
	public static ReadBody.Builder buildReadBody(String fname, int fId, int chunkId, int chunkSize) {
		ReadBody.Builder rb = ReadBody.newBuilder();
		if (fname != null) rb.setFilename(fname);
		if (fId != -1) rb.setFileId(fId);
		if (chunkId != -1) rb.setChunkId(chunkId);
		if (chunkSize != -1) rb.setChunkSize(chunkSize);
		return rb;
	}
	
	public static WriteBody.Builder buildWriteBody(int fId, String fname, String ext, Chunk.Builder chunk, int noChunks) {
		WriteBody.Builder wb = WriteBody.newBuilder();
		if (fId != -1) wb.setFileId(fId);
		wb.setFilename(fname);
		if (ext != null) wb.setFileExt(ext);
		if (chunk != null) wb.setChunk(chunk);
		if (noChunks != -1) wb.setNumOfChunks(noChunks);
		return wb;
	}
	
	public static Request.Builder buildRequest(TaskType t, WriteBody.Builder wb, ReadBody.Builder rb) {
		Request.Builder r = Request.newBuilder();
		if (wb != null) r.setRwb(wb);
		if (rb != null) r.setRrb(rb);
		return r;
	}
	
	public static Header.Builder buildHeader(int nodeId, long time) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(time);
		hb.setDestination(-1);
		return hb;
	}
	
	public static CommandMessage buildCommandMessage(Header.Builder h, Boolean ping, Request.Builder r, Response.Builder res) {
		CommandMessage.Builder cm = CommandMessage.newBuilder();
		cm.setHeader(h);
		if (ping != null) cm.setPing(ping);
		if (r != null) cm.setRequest(r);
		if (res != null) cm.setResponse(res);
		return cm.build();
	}
	
		
}
