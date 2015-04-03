package poke.server.election.raft;

import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.log.BufferedLog;
import poke.server.election.raft.state.CandidateState;
import poke.server.election.raft.state.FollowerState;
import poke.server.election.raft.state.LeaderState;
import poke.server.election.raft.state.RaftState;

public class Raft implements Election {
	protected static Logger logger = LoggerFactory.getLogger("raft");
	
	public enum State {
		Follower, Candidate, Leader;
	}

	RaftState followerState;
	RaftState candidateState;
	RaftState leaderState;
	RaftState currentState;
	
	Long lastKnownBeat;
	int term = 0;
	int votedFor = -1;

	int leaderId;
	int voteRecieved; // vote counter
	Integer nodeId;
	int totalNodes = 1;
	BufferedLog log; // also includes commitIndex and lastApplied
	ElectionListener listener;
	TreeMap<Integer, Long> nextIndex;
	TreeMap<Integer, Long> matchIndex;
	
	public Raft() {
		log = new BufferedLog();
		lastKnownBeat = System.currentTimeMillis();
		
		followerState = new FollowerState(this);
		candidateState = new CandidateState(this);
		leaderState = new LeaderState(this);
		currentState = followerState;
	}

	public Raft(Integer nodeId) {
		this.nodeId = nodeId;
		log = new BufferedLog();
		lastKnownBeat = System.currentTimeMillis();
		
		followerState = new FollowerState(this);
		candidateState = new CandidateState(this);
		leaderState = new LeaderState(this);
		currentState = followerState;
	}

	@Override
	public Management process(Management req) {
		return currentState.process(req);
	}
	
	public Management sendAppendResponse(int toNode, boolean responseFlag) {
		
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setToNode(toNode);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPENDRESPONSE);
		rmb.setResponseFlag(responseFlag);
		rmb.setTerm(this.term);
		
		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
	}

	public Management sendVoteResponse(int toNode, boolean responseFlag) {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setToNode(toNode);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.VOTE);
		rmb.setResponseFlag(responseFlag);
		rmb.setTerm(this.term);
		
		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
		
	}

	public Management sendLeaderNotice(){
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.LEADERNOTICE);
		rmb.setTerm(this.term);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
		
	}
	
	public Management getRequestVoteNotice() {
		logger.info("Node "+ getNodeId()+ " becomes candidate");
		setState(State.Candidate);
		setTerm(getTerm() + 1);
		setVoteRecieved(1);
		setVotedFor(getNodeId());
		setLastKnownBeat(System.currentTimeMillis());
		
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.REQUESTVOTE);
		rmb.setTerm(getTerm());
		//rmb.setPrevLogIndex(1);
		//rmb.setPrevTerm(this.term);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
		
	}
		
	public Management getAppendRequest() {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPEND);
		rmb.setTerm(this.term);
		//rmb.setPrevLogIndex(1);
		//rmb.setPrevTerm(this.term);
		//rmb.setEntries(value);
		//rmb.setLogCommitIndex(value);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
	}
	
	public void setState(State nextState) {
		switch(nextState) {
			case Follower:	currentState = followerState; break;
			case Leader: 	currentState = leaderState; break;
			case Candidate:	currentState = candidateState; break;
		}
	}
	
	public RaftState getState() {
		return currentState;
	}
	
	public int getVotedFor() {
		return votedFor;
	}

	public void setVotedFor(int votedFor) {
		this.votedFor = votedFor;
	}
	public int getTerm() {
		return term;
	}

	public void setTerm(int term) {
		this.term = term;
	}

	public Integer getNodeId() {
		return nodeId;
	}

	@Override
	public void setNodeId(int nodeId) {
		// TODO Auto-generated method stub
        this.nodeId = nodeId;
	}
	
	public int getTotalNodes() {
		return totalNodes;
	}

	public void setTotalNodes(int totalNodes) {
		this.totalNodes = totalNodes;
	}

	public Long getLastKnownBeat() {
		return lastKnownBeat;
	}

	public void setLastKnownBeat(Long lastKnownBeat) {
		this.lastKnownBeat = lastKnownBeat;
	}
	
	@Override
	public void setListener(ElectionListener listener) {
		// TODO Auto-generated method stub
		this.listener = listener;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isElectionInprogress() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Integer getElectionId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer createElectionID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getWinner() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getVoteRecieved() {
		return voteRecieved;
	}

	public void setVoteRecieved(int voteRecieved) {
		this.voteRecieved = voteRecieved;
	}

	public int getLeaderId() {
		return leaderId;
	}

	public void setLeaderId(int leaderId) {
		this.leaderId = leaderId;
	}

}