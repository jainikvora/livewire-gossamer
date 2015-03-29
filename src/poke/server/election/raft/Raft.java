package poke.server.election.raft;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.log.BufferedLog;

public class Raft implements Election {

	public Raft() {
		log = new BufferedLog();
	}

	public Raft(Integer nodeId) {
		this.nodeId = nodeId;
		log = new BufferedLog();
	}

	public enum RaftState {
		Follower, Candidate, Leader;
	}

	private RaftState currentState = RaftState.Follower;

	// Timer
	// RaftMonitor
	private int timeOut = 1000;
    private Long lastKnownBeat;
	private int term= 0;
	private int votedFor;
	private int leaderId;
	private int voteRecieved; // vote counter
	private Integer nodeId;
	private int totalNodes =1;
	private BufferedLog log;
	
	
	public int getTotalNodes() {
		return totalNodes;
	}

	public void setTotalNodes(int totalNodes) {
		this.totalNodes = totalNodes;
	}

	private ElectionListener listener;

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
	
	

	@Override
	public Management process(Management req) {
		RaftMessage msg = req.getRaftMessage();
		MgmtHeader header = req.getHeader();
		switch (msg.getAction()) {
		
		case REQUESTVOTE:
						if (this.currentState.equals(RaftState.Candidate)) {
							if (header.getOriginator() == getNodeId() /*&& leaderID == -1*/) {
		
								System.out.println("Node " + getNodeId()
										+ "gives vote to itself....");
								this.voteRecieved++;
								if (voteRecieved > this.totalNodes / 2){
									this.leaderId = getNodeId();
									voteRecieved = 0;
									//leaderTTL = 0;
									currentState = RaftState.Leader;
									
									return sendLeaderNotice();
									}
							} else if (header.getOriginator() != getNodeId()) {
								
								if(msg.getTerm()>this.term){
									this.currentState = RaftState.Follower;
								}
								// I'm a better candidate
								/*System.out
										.println("--> node "
												+ getNodeId()
												+ " is in candidate state, so ignoring request vote from  "
												+ header.getOriginator());*/
		
							}
						} else if (this.currentState.equals(RaftState.Leader)) {
		
						} else {
							if(msg.getTerm() > this.term){
								System.out.println("Node "+getNodeId()+" sending Vote to Node "+header.getOriginator());
								return sendVoteResponse(header.getOriginator(),true);  //true -- for vote granted
								//this.currentState = RaftState.Follower;
							}
							else{
								return sendVoteResponse(header.getOriginator(),false); // false - for vote rejection
							}
							
						}
						break;
						
		case VOTE:	
					if(this.currentState.equals(RaftState.Candidate)){
						
						if(msg.getTerm() > this.term) {
							this.currentState = RaftState.Follower;
			        	} else if(msg.getResponseFlag() == true) {
			        		this.voteRecieved++;
							System.out.println("Node "+getNodeId()+" received Vote from Node "+header.getOriginator());
							if (voteRecieved > this.totalNodes / 2){
								sendLeaderNotice();
								this.leaderId = getNodeId();
								voteRecieved = 0;
								currentState = RaftState.Leader;
								//leaderTTL = 0;
								System.out.println("Node "+getNodeId()+" declares itself as a Leader..");
							}
			        	}
					}
					
					if(this.currentState == RaftState.Candidate){
			        	
		            }
					break;
					
		case LEADERNOTICE:					
					System.out.println("--> node " + getNodeId()
							+ " acknowledges the leader is " + header.getOriginator());
	
					// things to do when we get a HB
					this.leaderId = header.getOriginator();
					lastKnownBeat = System.currentTimeMillis();
					currentState = RaftState.Follower;
					if(msg.getTerm() > this.term) {
						this.term = msg.getTerm();
					}
					//beatCounter = 0;
					break;
					
		case APPEND:
					if(this.currentState==RaftState.Follower){  //heartbeat check from Leader
						if(msg.getTerm() < this.term){
							return sendAppendResponse(header.getOriginator(), false); //false --> does not have updated data - send false to leader
						} else {
							System.out.println("Heartbeat check for Node "+getNodeId()+" from Leader Node "+header.getOriginator());
							//leaderID = msg.getOriginator();
							lastKnownBeat = System.currentTimeMillis();
							currentState = RaftState.Follower;
							//beatCounter = 0;
							return sendAppendResponse(header.getOriginator(), true);  // reply to Leader - append successful
						}
						
					}
					else if(this.currentState.equals(RaftState.Leader)){
						// Ignore 
						
					}
					break;
					
		case APPENDRESPONSE:
					 if(this.currentState.equals(RaftState.Leader)){
						//Acknowledgment from the follower node 
						if(msg.getTerm()>this.term){
								this.currentState = RaftState.Follower;
						} else if(msg.getResponseFlag() == false) {
							// if reply from follower is false to append request, decrement match index and send append entry again.
						} else {
							System.out.println("Acknowledgment from the follower node "+header.getOriginator()+" for Leader Node "+getNodeId());
							// check if commit index is increased
						}
						
					}
					break;
					
		default:
					break;
		}
		return null;
	}
	
	private Management sendAppendResponse(int toNode, boolean responseFlag) {
		
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setToNode(toNode);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPENDRESPONSE);
		rmb.setResponseFlag(responseFlag);
		rmb.setTerm(this.term);
		
		/*VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);
*/
		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
	}

	private Management sendVoteResponse(int toNode, boolean responseFlag) {
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setToNode(toNode);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.VOTE);
		rmb.setResponseFlag(responseFlag);
		rmb.setTerm(this.term);
		
		/*VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);
*/
		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
		
	}

	private Management sendLeaderNotice(){
		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(this.nodeId);
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.LEADERNOTICE);
		rmb.setTerm(this.term);
		
		/*VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);
*/
		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());
		
		return mb.build();
		
	}
	
	private void sendRequestVoteNotice() {
		
	}
	
	private void sendAppendRequest() {
		
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

	public void setNodeId(Integer nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public void setNodeId(int nodeId) {
		// TODO Auto-generated method stub
        this.nodeId = nodeId;
	}

}