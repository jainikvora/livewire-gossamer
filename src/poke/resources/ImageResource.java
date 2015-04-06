package poke.resources;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

import com.google.protobuf.ByteString;

import poke.comm.Image.Header;
import poke.comm.Image.PayLoad;
import poke.comm.Image.Ping;
import poke.comm.Image.Request;
import poke.core.Mgmt.DataSet;
import poke.core.Mgmt.LogEntry;
import poke.core.Mgmt.LogEntryList;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.NameValueSet;
import poke.core.Mgmt.RaftMessage;
import poke.resources.data.ClientInfo;
import poke.resources.data.MgmtResponse;
import poke.server.management.ManagementQueue;
import poke.server.queue.PerChannelQueue;

public class ImageResource implements ClientResource {

	private Map<Integer, ClientInfo> clientMap;

	private Map<Integer, ClientInfo> clusterMap;
	
	private static ImageResource imageResource;

	private ImageResource() {
		clientMap = new HashMap<Integer, ClientInfo>();
		clusterMap = new HashMap<Integer, ClientInfo>();
	}
	
	public ImageResource getInstance(){
		if(imageResource==null)
			this.imageResource= new ImageResource();
			
			return imageResource;
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

		boolean stored = storeImageInS3(request);
		if(stored){
			Management mgmtMessage = buildMgmtMessage(request);
			ManagementQueue.enqueueRequest(mgmtMessage,null);
		}

	}

	private boolean storeImageInS3(Request request) {
		// logic for storing image in s3
		return true;
	}
	
	private Request getImageFromS3(MgmtResponse mgmt) throws IOException{
		
		String imageKey = mgmt.getDataSet().getDataSet().getValue();
		//call get image from DAO
		//DAO will load image to temp folder
		//fetching image file from temp folder.
		byte[] myByeImage;
		BufferedImage originalImage = ImageIO.read(new File(
				"/home/ampatel/Downloads/"+imageKey+".jpg"));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(originalImage, "jpg", baos);
		baos.flush();
		myByeImage = baos.toByteArray();
		baos.close();
		ByteString bs = ByteString.copyFrom(myByeImage);
		return buildRequestMessage(mgmt,bs);
	}
	
	private Management buildMgmtMessage(Request request){
				
		NameValueSet.Builder nameAndValue = NameValueSet.newBuilder();
		nameAndValue.setName(request.getHeader().getCaption());
		nameAndValue.setValue(request.getPayload().getReqId());
		
		
		DataSet.Builder dataSet = DataSet.newBuilder();
		dataSet.setKey(request.getPayload().getReqId());
		if(request.getHeader().getIsClient())
			dataSet.setClientId(request.getHeader().getClientId());
		else
			dataSet.setClientId(-1);
		dataSet.setDataSet(nameAndValue.build());
		
		LogEntry.Builder logEntry = LogEntry.newBuilder();
		logEntry.setAction(LogEntry.DataAction.INSERT);
		logEntry.setTerm(-1);
		logEntry.setData(dataSet.build());
		
		LogEntryList.Builder logList = LogEntryList.newBuilder();
		logList.addEntry(logEntry.build());
		
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.CLIENTREQUEST);
		rmb.setEntries(logList.build());
		rmb.setTerm(-1);

		Management.Builder mb = Management.newBuilder();
		mb.setRaftMessage(rmb.build());

		return mb.build();
		
	}
	
	private Request buildRequestMessage(MgmtResponse mgmt,ByteString bs){
		
		Request.Builder r = Request.newBuilder();	
		
		PayLoad.Builder p = PayLoad.newBuilder();
		p.setReqId(mgmt.getDataSet().getKey());
		p.setData(bs);
		
		r.setPayload(p.build());

		// header with routing info
		Header.Builder h = Header.newBuilder();
		h.setClientId(mgmt.getDataSet().getClientId());
		h.setCaption(mgmt.getDataSet().getDataSet().getName());
		h.setIsClient(false);
		
		r.setHeader(h.build());
		
		Ping.Builder pg = Ping.newBuilder();
		pg.setIsPing(false);
		
		r.setPing(pg.build());

	    return r.build();	  
		
	}
	
	public void processRequestFromMgmt(List<MgmtResponse> list){
		
		Request imageResponse = null;
		for(MgmtResponse mgmt:list){
			
			try {
				imageResponse = getImageFromS3(mgmt);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(mgmt.getDataSet().getClientId()!=-1)
				sendImageToClusters(mgmt,imageResponse);
			sendImageToClients(mgmt,imageResponse);
			
		}
		
	}
	
	private void sendImageToClients(MgmtResponse mgmt,Request imageResponse){
		
		Iterator<Entry<Integer,ClientInfo>> iterator = clientMap.entrySet().iterator();
		while(iterator.hasNext()){			
			Entry<Integer,ClientInfo> entry = (Entry<Integer, ClientInfo>) iterator.next();
			if(entry.getKey()!=mgmt.getDataSet().getClientId() && entry.getValue().getLastSentIndex()<mgmt.getLogIndex()){
				clientMap.get(entry.getKey()).getChannel().getOutbound().add(imageResponse);
			}
		}
		
	}
	
	private void sendImageToClusters(MgmtResponse mgmt,Request imageResponse){
		
		Iterator<Entry<Integer,ClientInfo>> iterator = clusterMap.entrySet().iterator();
		while(iterator.hasNext()){			
			Entry<Integer,ClientInfo> entry = (Entry<Integer, ClientInfo>) iterator.next();
			clusterMap.get(entry.getKey()).getChannel().getOutbound().add(imageResponse);
		}
		
	}

}
