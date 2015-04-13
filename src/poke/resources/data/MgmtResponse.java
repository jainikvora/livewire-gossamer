package poke.resources.data;

import poke.core.Mgmt.DataSet;

public class MgmtResponse {
	private long logIndex;
	private DataSet dataSet;
	private boolean isLeader;
	
	

	public long getLogIndex() {
		return logIndex;
	}

	public void setLogIndex(long logIndex) {
		this.logIndex = logIndex;
	}

	public DataSet getDataSet() {
		return dataSet;
	}

	public void setDataSet(DataSet dataSet) {
		this.dataSet = dataSet;
	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

}
