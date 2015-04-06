package poke.resources;

import java.util.HashMap;
import java.util.Map;

import poke.comm.Image.Request;
import poke.resources.data.ClientInfo;
import poke.server.queue.PerChannelQueue;

public class ImageResource implements ClientResource {

	private Map<Integer, ClientInfo> clientMap;

	private Map<Integer, ClientInfo> clusterMap;

	public ImageResource() {
		clientMap = new HashMap<Integer, ClientInfo>();
		clusterMap = new HashMap<Integer, ClientInfo>();
	}

	private synchronized void addClient(Integer clientID, ClientInfo clientInfo) {
		clientMap.put(clientID, clientInfo);
	}

	private synchronized void addCluster(Integer clusterId,
			ClientInfo clientInfo) {
		clusterMap.put(clusterId, clientInfo);
	}

	private synchronized Map<Integer, ClientInfo> getClientMap() {
		return clientMap;
	}

	private synchronized Map<Integer, ClientInfo> getClusterMap() {
		return clusterMap;
	}

	/*
	 * public void setClientMap(Map<String, Channel> clientMap) { this.clientMap
	 * = clientMap; }
	 */

	public ClientInfo getClientInfo(int clientId) {
		return clientMap.get(clientId);
	}

	public ClientInfo getClusterInfo(int clusterId) {
		return clusterMap.get(clusterId);
	}

	@Override
	public void process(Request request, PerChannelQueue channel) {

		boolean isClient = request.getHeader().getIsClient();
		if (isClient) {
			Integer clientId = request.getHeader().getClientId();
			if (!getClientMap().containsKey(clientId)) {
				addClient(clientId, new ClientInfo(channel, 0, false));
			} else {
				getClientInfo(clientId).setChannel(channel);
			}
		}

		else {

			Integer clusterId = request.getHeader().getClusterId();
			if (!getClusterMap().containsKey(clusterId)) {
				addCluster(clusterId, new ClientInfo(channel, 0, true));
			} else {
				getClusterInfo(clusterId).setChannel(channel);
			}

		}

		String storedImageURL = storeImageInS3(request);
		
		//mgmt message build
		//MAnagementQueue.put
		//

	}

	private String storeImageInS3(Request request) {
		// logic for storing image in s3
		return "image123";
	}

}
