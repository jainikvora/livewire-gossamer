package poke.resources;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

import poke.comm.App.Request;
import poke.server.resources.Resource;

public class ClientResource implements Resource{
	
	private Map<String,Channel> clientMap;
	
	public ClientResource(){
		clientMap = new HashMap<String,Channel>();
	}
	
	public void addClient(String clientID,Channel channel){
		clientMap.put(clientID, channel);
	}
	
	public Channel getClientChannel(String clientID){
		return clientMap.get(clientID);
	}

	@Override
	public Request process(Request request) {
		// TODO Auto-generated method stub
		return null;
	}

}
