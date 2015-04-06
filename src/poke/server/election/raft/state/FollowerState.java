package poke.server.election.raft.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.LogEntry;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.raft.Raft;

public class FollowerState extends RaftState {
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
			if (msg.getTerm() < raft.getTerm()) {
				logger.info("Node " + raft.getNodeId()
						+ " rejecting Vote to Node " + header.getOriginator());
				response = raft.getVoteResponse(header.getOriginator(), false);
			}
			/** 
			 * Give vote if candidate's log is at least up to date with reciever's log
			 */
			else if (raft.getVotedFor() == -1 || raft.getVotedForInTerm() != msg.getTerm()) {
				if(msg.getPrevLogIndex() >= raft.getLog().lastIndex()) {
					if(raft.getLog().getEntry(msg.getPrevLogIndex()) != null) {
						if(raft.getLog().getEntry(msg.getPrevLogIndex()).getTerm() != msg.getPrevTerm()) {
							response = raft.getVoteResponse(header.getOriginator(), false);
						} else {
							response = raft.getVoteResponse(header.getOriginator(), true);
						}
					} else {
						raft.setVotedFor(header.getOriginator());
						raft.setVotedForInTerm(msg.getTerm());
						logger.info("Node " + raft.getNodeId()
								+ " sending Vote to Node " + header.getOriginator());
						response = raft.getVoteResponse(header.getOriginator(), true);
					}
				} else {
					response = raft.getVoteResponse(header.getOriginator(), false);
				}
			}
			raft.setLastKnownBeat(System.currentTimeMillis());
			break;
		case VOTE:
			break;
		case APPEND:
			if (msg.getTerm() < raft.getTerm()) {
				/* leader has stale entries - reject append */
				response = raft
						.getAppendResponse(header.getOriginator(), false);
			}
			/**
			 * Reply false if log doesn't contain an entry at prevLogIndex
			 * whose term matches prevLogTerm
			 */
			else if (msg.hasEntries()) {
				LogEntry prevEntry = raft.getLog().getEntry(
						msg.getPrevLogIndex());
				
				if(prevEntry == null) {
					response = raft.getAppendResponse(header.getOriginator(),
							false);
				} else if (prevEntry.getTerm() != msg.getPrevTerm()) {
					response = raft.getAppendResponse(header.getOriginator(),
							false);
				}
				/**
				 * If an existing entry conflicts with a new one (same index
				 * but different terms), delete the existing entry and all
				 * that follow it.
				 */
				else {
					if (msg.getPrevLogIndex() != raft.getLog().lastIndex()) {
						long logMatchIndex = raft.getLog().lastIndex();

						while (msg.getPrevLogIndex() != logMatchIndex
								|| msg.getPrevTerm() != raft.getLog()
										.getEntry(logMatchIndex).getTerm()) {
							logMatchIndex -= 1;
						}
						raft.getLog().removeEntry(logMatchIndex);
					}
					/* Append any new entries not already in the log */
					LogEntry[] recievedEntries = new LogEntry[msg.getEntries()
							.getEntryList().size()];
					recievedEntries = msg.getEntries().getEntryList().toArray(recievedEntries);
					raft.getLog().appendEntry(recievedEntries);

					/* If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
					if (msg.getLogCommitIndex() > raft.getLog()
							.getCommitIndex()) {
						long newCommitIndex = Math.min(msg.getLogCommitIndex(),
								raft.getLog().lastIndex());
						//raft.getLog().setCommitIndex(newCommitIndex);
						raft.updateCommitIndex(newCommitIndex);
					}
				}
			}
			/**
			 * Reply false if log's lastIndex and Term doesn't match with
			 * prevLogInxed and prevTerm.
			 */
			else if (msg.getPrevLogIndex() != raft.getLog().lastIndex()
					&& msg.getPrevTerm() != raft.getLog()
							.getEntry(raft.getLog().lastIndex()).getTerm()) {
				response = raft
						.getAppendResponse(header.getOriginator(), false);
			}
			/**
			 * Fully replicated with leader - send response to
			 * heartbeat message
			 */
			else {
				if (raft.getLeaderId() != header.getOriginator()) {
					raft.setLeaderId(header.getOriginator());
					raft.setVotedFor(-1);
					logger.info("Node " + raft.getNodeId()
							+ " acknowledges the leader is "
							+ header.getOriginator());
				}

				if (msg.getTerm() > raft.getTerm()) {
					raft.setTerm(msg.getTerm());
				}

				logger.info("Node " + raft.getNodeId()
						+ " acknowledges heartbeat from Node "
						+ header.getOriginator());

				response = raft.getAppendResponse(header.getOriginator(), true);
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
