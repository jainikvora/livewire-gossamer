package poke.server.election.raft.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.raft.Raft;

public class FollowerState extends RaftState{
	protected static Logger logger = LoggerFactory.getLogger("raftManager:followerState");
	
	public FollowerState(Raft raft) {
		super(raft);
	}

	@Override
	public Management process(Management req) {
		RaftMessage msg = req.getRaftMessage();
		MgmtHeader header = req.getHeader();
		
		Management response = null;
		
		switch (msg.getAction()) {
			case REQUESTVOTE:
				if(msg.getTerm() < raft.getTerm()){
					logger.info("Node "+ raft.getNodeId() + " rejecting Vote to Node "+header.getOriginator());
					response = raft.sendVoteResponse(header.getOriginator(),false); // false - for vote rejection
					raft.setLastKnownBeat(System.currentTimeMillis());
				}
				else if(raft.getVotedFor() == -1){
					raft.setVotedFor(header.getOriginator());
					logger.info("Node "+ raft.getNodeId()+ " sending Vote to Node "+header.getOriginator());
					response = raft.sendVoteResponse(header.getOriginator(),true);  //true -- for vote granted
					raft.setLastKnownBeat(System.currentTimeMillis());
				}
				
				break;
			case VOTE:
				break;
			case APPEND:
				if(raft.getTerm() > msg.getTerm()) {
					// leader has stale entries - reject append
					response = raft.sendAppendResponse(header.getOriginator(), false);
				} else if(msg.getEntries().getEntryCount() > 0) {
					/**
					 * Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
					 * If an existing entry conflicts with a new one (same index but different terms),
					 * delete the existing entry and all that follow it.
					 * 
					 * Append any new entries not already in the log
					 * If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
					 */
					
				} /*else if(msg.getTerm() > raft.getTerm()) {
					*//**
					 * Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
					 * else update term = msg.getTerm();
					 *//*
				}*/ else {
					if(raft.getLeaderId() != header.getOriginator()) {
						raft.setLeaderId(header.getOriginator());
						raft.setVotedFor(-1);
						logger.info("Node "+ raft.getNodeId()+ " acknowledges the leader is " + header.getOriginator());
					}
					
					if(msg.getTerm() > raft.getTerm()) {
						raft.setTerm(msg.getTerm());
					}
					
					logger.info("Node "+ raft.getNodeId() + " acknowledges heartbeat from Node " + header.getOriginator());
					raft.setLastKnownBeat(System.currentTimeMillis());

					response = raft.sendAppendResponse(header.getOriginator(), true);  // reply to Leader - append successful
				}
				
				raft.setLastKnownBeat(System.currentTimeMillis());
				
				break;
			case APPENDRESPONSE:
				break;
			default:
				break;
		}
		
		return response;
	}

}
