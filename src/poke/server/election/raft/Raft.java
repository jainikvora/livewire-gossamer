package poke.server.election.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import poke.resources.ImageResource;
import poke.resources.data.MgmtResponse;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.log.BufferedLog;
import poke.server.election.raft.state.CandidateState;
import poke.server.election.raft.state.FollowerState;
import poke.server.election.raft.state.LeaderState;
import poke.server.election.raft.state.RaftState;
import poke.server.managers.ConnectionManager;
import poke.server.managers.RaftManager;

/**
 * 
 * Raft Election to ensure re-election when a leader fails and 
 * Log Replication to keep system consistent  
 *
 */
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
	
	// stores the next logIndex to send to each follower
	TreeMap<Integer, Long> nextIndex = new TreeMap<Integer, Long>();
	// stores the last logIndex sent to each follower
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

	/**
	 * Process RaftMessage - delegate to currentState
	 * if the request is ClientRequest,
	 * 	if currentState is Leader, send append request to followers
	 * 	else forward client request to Leader
	 */
	@Override
	public Management process(Management req) {
		if(req.getRaftMessage().getAction() == RaftMessage.Action.CLIENTREQUEST) {
			LogEntry entry = req.getRaftMessage().getEntries().getEntryList().get(0);
			boolean isInsert = entry.getAction() == LogEntry.DataAction.INSERT ? true : false; 
			appendLogEntry(entry.getData(), isInsert);
			return null;
		} else {
			Management res = currentState.process(req);
			
			return res;
		}
	}

	/**
	 * Build appendResponse to send to the leader node
	 * @param toNode
	 * @param responseFlag
	 * @return Management response
	 */
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

	/**
	 * Build vote response to send to candidate
	 * @param toNode
	 * @param responseFlag
	 * @return Management response
	 */
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

	/**
	 * Build voteRequest to send to other nodes in the cluster
	 * @return Management response
	 */
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
	
	/**
	 * Build AppendRequest for heartbeat i.e. with 0 log entries
	 * @return Management response
	 */
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
	
	/**
	 * Build AppendRequest to send to a follower with log entries
	 * starting from logStartIndex to lastIndx
	 * @param toNode
	 * @param logStartIndex
	 * @return Management response
	 */
	public Management getAppendRequest(int toNode, long logStartIndex) {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPEND);
		rmb.setTerm(this.term);
		rmb.setPrevLogIndex(logStartIndex - 1);
		//logger.info("Log Start Index: " + logStartIndex);
		rmb.setPrevTerm((logStartIndex - 1) != 0 ? log.getEntry(logStartIndex - 1)
				.getTerm() : 0);
		rmb.setLogCommitIndex(log.getCommitIndex());

		LogEntryList.Builder le = LogEntryList.newBuilder();
		//logger.info("Current entries in log:"+ log.getEntries(logStartIndex).length);
		le.addAllEntry(Arrays.asList(log.getEntries(logStartIndex)));
		rmb.setEntries(le.build());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		return mb.build();
	}

	/**
	 * Build AppendRequests for all the nodes with entries starting 
	 * from nextIndex for that node
	 * @return Map<Integer, Management>
	 */
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
	
	/**
	 * Insert a new logEntry in log, send append request to all
	 * followers to apped new log entry
	 * @param data
	 * @param isInsert
	 * @return 
	 */
	public void appendLogEntry(DataSet data, boolean isInsert) {
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
		else if(currentState instanceof FollowerState){
			// send to leader
			forwardClientRequestToLeader(data);
		} else {
			// TODO what to do when node is in candidate state
		}
	}

	/**
	 * Determine if a candidate has won election after a Candidate
	 * receives a vote
	 * @return true if vote received from majority, false otherwise
	 */
	public boolean hasWonElection() {
		if (getVoteRecieved() > getTotalNodes() / 2) {
			return true;
		}
		return false;
	}
	
	/**
	 * If Candidate has won election, declare itself as leader by
	 * sending AppendRequest to all followers and reinitialize all
	 * followers
	 * @return Management 
	 */

	public Management declareLeader() {
		setState(State.Leader);
		setLeaderId(getNodeId());
		setVoteRecieved(0);
		setVotedFor(-1);
		setVotedForInTerm(0);
		logger.info("Node " + getNodeId() + " declares itself as a Leader ");
		reinitializeIndexes();
		
		// notifying ImageResource that the node is leader
		ImageResource.getInstance().setLeader();
		List<MgmtResponse> resourceList = new ArrayList<MgmtResponse>();
		MgmtResponse response = new MgmtResponse();
		response.setLeader(true);
		resourceList.add(response);
		ImageResource.getInstance().processRequestFromMgmt(resourceList);
		
		return getAppendRequest();
	}

	/**
	 * After transition to leader, For each follower, reinitialize nextIndex 
	 * to lastIndex+1 and matchIndex to 0
	 */
	private void reinitializeIndexes() {
		for (Integer nodeId : ConnectionManager.getConnectedNodeSet(true)) {
			// Next Index
			nextIndex.put(nodeId, log.lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (long) 0);
		}
	}
	
	/**
	 * Checks If there exists an N such that N > commitIndex, a majority 
	 * of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	 * set commitIndex = N
	 * 
	 * if commitIndex gets updated send message to Resource with 
	 * entries starting from lastApplied + 1 to commit index
	 */
	public void updateCommitIndex() {
		long max = Long.MIN_VALUE;
		for(Long index : matchIndex.values()) {
			if(index > max)
				max = index;
		}
		
		int occurence = 1;
		boolean majority = false;
		long N = max;
		
		while(N > log.getCommitIndex() && !majority) {
			for(Long index : matchIndex.values()) {
				if(index >= N)
					occurence += 1;
			}
			if(occurence >= totalNodes/2) {
				majority = true;
			} else {
				N -= 1;
				occurence = 0;
			}
		}
		
		if(N > 0) {
			if(log.getEntry(N).getTerm() == this.getTerm() && N > log.getCommitIndex()) {
				log.setCommitIndex(N);
				// send message to Resource with entries starting from 
				// lastApplied + 1 to commit index
				if(log.getCommitIndex() > log.getLastApplied()) {
					List<MgmtResponse> resourceList = new ArrayList<MgmtResponse>();
					for(long i = log.getLastApplied() + 1; i <= log.getCommitIndex(); i++) {
						MgmtResponse response = new MgmtResponse();
						response.setLogIndex(i);
						response.setDataSet(log.getEntry(i).getData());
						response.setLeader(false);
						resourceList.add(response);
					}
					ImageResource.getInstance().processRequestFromMgmt(resourceList);
					log.setLastApplied(log.getCommitIndex());
				}
			}
		}
		
	}
	
	/**
	 * When in FollowerState, if commitIndex gets updated by leader,
	 * update the commitIndex of self and send message to Resource with 
	 * entries starting from lastApplied + 1 to commit index
	 * @param newCommitIndex
	 */
	public void updateCommitIndex(long newCommitIndex) {
		log.setCommitIndex(newCommitIndex);
		if(log.getCommitIndex() >log.getLastApplied()) {
			List<MgmtResponse> resourceList = new ArrayList<MgmtResponse>();
			for(long i = log.getLastApplied() + 1 ; i <= log.getCommitIndex(); i++) {
				MgmtResponse response = new MgmtResponse();
				response.setLogIndex(i);
				response.setDataSet(log.getEntry(i).getData());
				response.setLeader(false);
				resourceList.add(response);
			}
			ImageResource.getInstance().processRequestFromMgmt(resourceList);
			log.setLastApplied(log.getCommitIndex());
		}
	}
	
	/**
	 * When AppendRequest is successful for a node, update it's 
	 * nextIndex to lastIndex+1 and matchIndex to lastIndex
	 * @param nodeId
	 */
	public void updateNextAndMatchIndex(int nodeId) {
		if(!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, log.lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (long) 0);
		}
		
		if(matchIndex.get(nodeId) != log.lastIndex()) {
			nextIndex.put(nodeId, log.lastIndex() + 1);
			matchIndex.put(nodeId, log.lastIndex());
			// check if commit index is also increased
			updateCommitIndex();
		}
	}
	
	/**
	 * If follower rejects an AppendRequest due to log mismatch,
	 * decrement the follower's next index and return the
	 * decremented index
	 * @param nodeId
	 * @return decremented nextIndex
	 */
	public long getDecrementedNextIndex(int nodeId) {
		nextIndex.put(nodeId, nextIndex.get(nodeId) - 1);
		return nextIndex.get(nodeId);
	}
	
	/**
	 * If current state is follower and client send as msg to broadcast,
	 * forward the msg as ClientRequest to the current leader
	 * @param DataSet data
	 */
	public void forwardClientRequestToLeader(DataSet data) {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.CLIENTREQUEST);
		rmb.setTerm(-1);
		
		LogEntry.Builder leb = LogEntry.newBuilder();
		leb.setAction(LogEntry.DataAction.INSERT);
		leb.setData(data);
		leb.setTerm(-1);
		
		LogEntryList.Builder le = LogEntryList.newBuilder();
		le.addEntry(leb.build());
		rmb.setEntries(le.build());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		RaftManager.getInstance().sendMgmtRequest(leaderId, mb.build());
	}

	/**
	 * Set CurrentState
	 * @param nextState
	 */
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

	/**
	 * Get currentState
	 * @return RaftState currentState
	 */
	public RaftState getState() {
		return currentState;
	}
	
	/**
	 * Get currentState in format of State
	 * To be used by Application Resource
	 * @return State currentState
	 */
	public State getCurrentState() {
		State state = State.Follower;
		if(currentState instanceof LeaderState) {
			state = State.Leader;
		} else if(currentState instanceof CandidateState) {
			state = State.Candidate;
		}
		return state;
	}
	
	/**
	 * Send logs to resource starting from startIndex.
	 * To reduce message traffic to client, send only last 10 entries
	 * if lastIndex - startIndex > 10
	 * @param startIndex
	 * @return
	 */
	public List<MgmtResponse> getDataSetFromLog(long startIndex) {
		int maxDataToSent = 10;
		List<MgmtResponse> resourceList = new ArrayList<MgmtResponse>();
		if(log.lastIndex() - startIndex > 10) {
			for(long i = log.lastIndex() - maxDataToSent; i <= log.lastIndex(); i++) {
				MgmtResponse response = new MgmtResponse();
				response.setLogIndex(i);
				response.setDataSet(log.getEntry(i).getData());
				resourceList.add(response);
			}
		} else {
			for(long i = startIndex; i <= log.lastIndex(); i++) {
				MgmtResponse response = new MgmtResponse();
				response.setLogIndex(i);
				response.setDataSet(log.getEntry(i).getData());
				resourceList.add(response);
			}
		}
		return resourceList;
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