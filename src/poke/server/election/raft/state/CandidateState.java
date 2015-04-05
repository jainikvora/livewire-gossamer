package poke.server.election.raft.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.raft.Raft;
import poke.server.election.raft.Raft.State;

public class CandidateState extends RaftState{
	protected static Logger logger = LoggerFactory.getLogger("raftManager:candidateState");
	
	public CandidateState(Raft raft) {
		super(raft);
	}

	@Override
	public Management process(Management req) {
		RaftMessage msg = req.getRaftMessage();
		MgmtHeader header = req.getHeader();
		Management response = null;
		
		switch (msg.getAction()) {
			case REQUESTVOTE:
				if(msg.getTerm() > raft.getTerm()) {
					raft.setVotedFor(header.getOriginator());
					raft.setVotedForInTerm(msg.getTerm());
					raft.setState(State.Follower);
					logger.info("Node "+ raft.getNodeId()+ " sending Vote to Node "+header.getOriginator());
					response = raft.getVoteResponse(header.getOriginator(),true);  //true -- for vote granted
				} /*else if(raft.getVotedFor() == raft.getNodeId()){
					// candidate’s log is at least as up-to-date as receiver’s log
					raft.setVotedFor(header.getOriginator());
					raft.setState(State.Follower);
					response = raft.getVoteResponse(header.getOriginator(),true);  //true -- for vote granted
				}*/ else {
					response = raft.getVoteResponse(header.getOriginator(),false);  //true -- for vote granted
				}
				raft.setLastKnownBeat(System.currentTimeMillis());
				break;
			case VOTE:
				if(msg.getTerm() > raft.getTerm()) {
					raft.setState(State.Follower);
					raft.setLastKnownBeat(System.currentTimeMillis());
					raft.setVotedFor(-1);
	        	} else if(msg.getResponseFlag() == true) {
	        		raft.setVoteRecieved(raft.getVoteRecieved() + 1);
	        		if(raft.hasWonElection()) {
						response = raft.declareLeader();
	        		}
	        	}
				raft.setLastKnownBeat(System.currentTimeMillis());
				break;
			case APPEND:
				if(msg.getTerm() > raft.getTerm()) {
					raft.setVotedFor(-1);
					raft.setState(State.Follower);
					response = raft.process(req);
				}
				break;
			case APPENDRESPONSE:
				break;
			default:
				break;
			}			
		return response;
	}

}
