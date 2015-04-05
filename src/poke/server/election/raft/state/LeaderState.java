package poke.server.election.raft.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.election.raft.Raft;
import poke.server.election.raft.Raft.State;

public class LeaderState extends RaftState {
	protected static Logger logger = LoggerFactory
			.getLogger("raftManager:leaderState");

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
			if (msg.getTerm() > raft.getTerm()) {
				raft.setState(State.Follower);
				logger.info("Node " + raft.getNodeId()
						+ " sending Vote to Node " + header.getOriginator());
				response = raft.getVoteResponse(header.getOriginator(), true);
			}
			break;
		case VOTE:
			break;
		case APPEND:
			break;
		case APPENDRESPONSE:
			/* Acknowledgment from the follower node */
			if (msg.getTerm() > raft.getTerm()) {
				raft.setState(State.Follower);
			}
			/**
			 * If AppendEntries fails because of log inconsistency:
			 * decrement nextIndex and retry
			 */
			else if (msg.getResponseFlag() == false) {
				response = raft.getAppendRequest(header.getOriginator(),
						raft.getDecrementedNextIndex(header.getOriginator()));
			}
			/* Append successful on a follower */
			else {
				raft.updateNextAndMatchIndex(header.getOriginator());
				logger.info("Acknowledgment from the follower node "
						+ header.getOriginator() + " for Leader Node "
						+ raft.getNodeId());
				// check if commit index is increased
			}
			break;
		default:
			break;
		}
		return response;
	}
}
