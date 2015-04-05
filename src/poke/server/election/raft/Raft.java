package poke.server.election.raft;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.DataSet;
import poke.core.Mgmt.LogEntry;
import poke.core.Mgmt.LogEntry.DataAction;
import poke.core.Mgmt.LogEntryList;
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
import poke.server.managers.ConnectionManager;
import poke.server.managers.RaftManager;

public class Raft implements Election {
	protected static Logger logger = LoggerFactory.getLogger("raft");

	public enum State {
		Follower, Candidate, Leader;
	}

	RaftState followerState;
	RaftState candidateState;
	RaftState leaderState;
	RaftState currentState;

	Long lastKnownBeat = System.currentTimeMillis();
	int term = 0;
	int votedFor = -1;
	int votedForInTerm = 0;

	int leaderId;
	int voteRecieved; // vote counter
	Integer nodeId;
	int totalNodes = 1;
	BufferedLog log; // also includes commitIndex and lastApplied
	ElectionListener listener;
	TreeMap<Integer, Long> nextIndex = new TreeMap<Integer, Long>();
	TreeMap<Integer, Long> matchIndex = new TreeMap<Integer, Long>();

	public Raft() {
		log = new BufferedLog();
		followerState = new FollowerState(this);
		candidateState = new CandidateState(this);
		leaderState = new LeaderState(this);
		currentState = followerState;
	}

	public Raft(Integer nodeId) {
		this.nodeId = nodeId;
		log = new BufferedLog();

		followerState = new FollowerState(this);
		candidateState = new CandidateState(this);
		leaderState = new LeaderState(this);
		currentState = followerState;
	}

	@Override
	public Management process(Management req) {
		return currentState.process(req);
	}

	public Management getAppendResponse(int toNode, boolean responseFlag) {

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

	public Management getVoteResponse(int toNode, boolean responseFlag) {
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

	public Management getRequestVoteNotice() {
		logger.info("Node " + getNodeId() + " becomes candidate");
		setState(State.Candidate);
		setTerm(getTerm() + 1);
		setVoteRecieved(1);
		setVotedFor(getNodeId());
		setVotedForInTerm(getTerm());
		setLastKnownBeat(System.currentTimeMillis());

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.REQUESTVOTE);
		rmb.setTerm(getTerm());
		rmb.setPrevLogIndex(log.lastIndex());
		rmb.setPrevTerm(log.lastIndex() != 0 ? log.getEntry(log.lastIndex())
				.getTerm() : 0);

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
		rmb.setPrevLogIndex(log.lastIndex());
		rmb.setPrevTerm(log.lastIndex() != 0 ? log.getEntry(log.lastIndex())
				.getTerm() : 0);
		// rmb.setEntries(value);
		rmb.setLogCommitIndex(log.getCommitIndex());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		return mb.build();
	}
	
	public Management getAppendRequest(int toNode, long logStartIndex) {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPEND);
		rmb.setTerm(this.term);
		rmb.setPrevLogIndex(logStartIndex - 1);
		rmb.setPrevTerm((logStartIndex - 1) != 0 ? log.getEntry(logStartIndex - 1)
				.getTerm() : 0);
		rmb.setLogCommitIndex(log.getCommitIndex());

		LogEntryList.Builder le = LogEntryList.newBuilder();
		le.addAllEntry(Arrays.asList(log.getEntries(logStartIndex)));
		rmb.setEntries(le.build());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		return mb.build();
	}

	public Map<Integer, Management> getAppendRequestForFollowers() {
		Map<Integer, Management> appendRequests = new HashMap<Integer, Management>();

		for (Entry<Integer, Long> nodeNextIndex : nextIndex.entrySet()) {
			if (log.lastIndex() >= nodeNextIndex.getValue()) {
				appendRequests.put(
						nodeNextIndex.getKey(),
						getAppendRequest(nodeNextIndex.getKey(),
								nodeNextIndex.getValue()));
			}
		}
		return appendRequests;
	}
	
	public boolean appendLogEntry(DataSet data, boolean isInsert) {
		if(currentState instanceof LeaderState) {
			LogEntry.Builder leb = LogEntry.newBuilder();
			if(isInsert) {
				leb.setAction(DataAction.INSERT);
				leb.setData(data);
				leb.setTerm(getTerm());
				log.appendEntry(leb.build());
			}
			
			RaftManager.getInstance().sendAppendRequest(getAppendRequestForFollowers());
			/*while(!isLogReplicated && leaderActive){};
			isReplicationInProcess = false; 
			isLogReplicated = false;*/
		}
		else {
			// send to leader
		}
		return true;
	}

	public boolean hasWonElection() {
		if (getVoteRecieved() > getTotalNodes() / 2) {
			return true;
		}
		return false;
	}

	public Management declareLeader() {
		setState(State.Leader);
		setLeaderId(getNodeId());
		setVoteRecieved(0);
		setVotedFor(-1);
		setVotedForInTerm(0);
		logger.info("Node " + getNodeId() + " declares itself as a Leader ");
		reinitializeIndexes();
		return getAppendRequest();
	}

	private void reinitializeIndexes() {
		for (Integer nodeId : ConnectionManager.getConnectedNodeSet(true)) {
			// Next Index
			nextIndex.put(nodeId, log.lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (long) 0);
		}
	}
	
	/**
	 * If there exists an N such that N > commitIndex, a majority 
	 * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	 * set commitIndex = N
	 */
	public void updateCommitIndex() {
		long max = Long.MIN_VALUE;
		for(Long index : matchIndex.values()) {
			if(index > max)
				max = index;
		}
		
		int occurence = 0;
		boolean majority = false;
		long N = max;
		
		while(N > log.getCommitIndex() && !majority) {
			for(Long index : matchIndex.values()) {
				if(index >= N)
					occurence += 1;
			}
			if(occurence > totalNodes/2) {
				majority = true;
			} else {
				N -= 1;
				occurence = 0;
			}
		}
		
		if(log.getEntry(N).getTerm() == this.getTerm() && N > log.getCommitIndex()) {
			log.setCommitIndex(N);
			// send message to Resource with entries starting from 
			// lastApplied + 1 to commit index
			log.setLastApplied(log.getCommitIndex());
		}
	}
	
	public void updateNextAndMatchIndex(int nodeId) {
		if(matchIndex.get(nodeId) != log.lastIndex()) {
			nextIndex.put(nodeId, log.lastIndex() + 1);
			matchIndex.put(nodeId, log.lastIndex());
			// check if commit index is also increased
			updateCommitIndex();
		}
	}
	
	public long getDecrementedNextIndex(int nodeId) {
		nextIndex.put(nodeId, nextIndex.get(nodeId) - 1);
		return nextIndex.get(nodeId);
	}

	public void setState(State nextState) {
		switch (nextState) {
		case Follower:
			currentState = followerState;
			break;
		case Leader:
			currentState = leaderState;
			break;
		case Candidate:
			currentState = candidateState;
			break;
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
		this.listener = listener;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isElectionInprogress() {
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

	public BufferedLog getLog() {
		return log;
	}

	public int getVotedForInTerm() {
		return votedForInTerm;
	}

	public void setVotedForInTerm(int votedForInTerm) {
		this.votedForInTerm = votedForInTerm;
	}
}