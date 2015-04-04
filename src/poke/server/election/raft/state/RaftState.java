package poke.server.election.raft.state;

import poke.core.Mgmt.Management;
import poke.server.election.raft.Raft;

public abstract class RaftState {
	Raft raft;
	
	public RaftState(Raft raft){
		this.raft = raft;
	}
	
	public abstract Management process(Management req);
}
