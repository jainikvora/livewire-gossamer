package poke.server.election.raft.log;

import poke.core.Mgmt.LogEntry;
/*
 * An interface defining basic operations any log must support.
 */
public interface Loggable {
	
	long size();
	
	long appendEntry(LogEntry entry);
	
	long firstIndex();
	
	long lastIndex();
	
	LogEntry getEntry(long index);
}
