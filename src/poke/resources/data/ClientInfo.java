package poke.resources.data;

import poke.server.queue.PerChannelQueue;

public class ClientInfo {
	
	private PerChannelQueue channel;
	
	private long lastSentIndex;
	
	private boolean isServer; 
	
	public boolean isServer() {
		return isServer;
	}

	public void setServer(boolean isServer) {
		this.isServer = isServer;
	}

	public ClientInfo(PerChannelQueue channel,long lastSentIndex,boolean isServer){
		this.channel = channel;
		this.lastSentIndex = lastSentIndex;
		this.isServer = isServer;
	}

	public PerChannelQueue getChannel() {
		return channel;
	}

	public void setChannel(PerChannelQueue channel) {
		this.channel = channel;
	}

	public long getLastSentIndex() {
		return lastSentIndex;
	}

	public void setLastSentIndex(long lastSentIndex) {
		this.lastSentIndex = lastSentIndex;
	}
	
	

}
