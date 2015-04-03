package poke.server.election.raft.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.raft.Raft;
import poke.server.election.raft.Raft.State;

public class LeaderState extends RaftState{
	protected static Logger logger = LoggerFactory.getLogger("raftManager:leaderState");
	
	public LeaderState(Raft raft) {
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
				raft.setState(State.Follower);
				logger.info("Node "+ raft.getNodeId()+ " sending Vote to Node "+header.getOriginator());
				response = raft.sendVoteResponse(header.getOriginator(),true);  //true -- for vote granted
			}
			break;
		case VOTE:
			break;
		case APPEND:
			break;
		case APPENDRESPONSE:
			//Acknowledgment from the follower node 
			if(msg.getTerm() > raft.getTerm()){
				raft.setState(State.Follower);
			} else if(msg.getResponseFlag() == false) {
				// if reply from follower is false to append request, first check the term of the follower
				// if it is > term, change to follower
				// decrement match index and send append entry again.
			} else {
				logger.info("Acknowledgment from the follower node "+ header.getOriginator() + " for Leader Node "+ raft.getNodeId());
				// check if commit index is increased
			}
			break;
		default:
			break;
		}	
		return response;
	}
}
