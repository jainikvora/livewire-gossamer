package poke.resources.data;

import poke.core.Mgmt.DataSet;

public class MgmtResponse {
	private long logIndex;
	private DataSet dataSet;

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

}
