package poke.resources;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

import javax.imageio.ImageIO;

import poke.comm.Image.Request;
import poke.core.Mgmt.Management;
import poke.resources.data.ClientInfo;
import poke.resources.data.MgmtResponse;
import poke.resources.data.DAO.ClientDAO;
import poke.resources.data.DAO.ImageStoreProxy;
import poke.resources.data.DAO.ImageStoreProxy.ImageStoreMethod;
import poke.server.conf.ServerConf;
import poke.server.conf.ServerList;
import poke.server.conf.ServerList.TCPAddress;
import poke.server.management.ManagementQueue;
import poke.server.managers.RaftManager;
import poke.server.queue.PerChannelQueue;
import poke.util.ChannelCreator;
import poke.util.MessageBuilder;

import com.google.protobuf.ByteString;

public class ImageResource extends Thread implements ClientResource {

	private Map<Integer, ClientInfo> clientMap;
	private Map<Integer, ClientInfo> clusterMap;
	private ServerConf conf;
	private boolean forever = true;
	private ServerList serverList = null;
	private boolean isLeader =false;
	private boolean clustersConnected = false;
	private String nodeId;

	private LinkedBlockingDeque<MgmtResponse> inbound = new LinkedBlockingDeque<MgmtResponse>();

	private static ImageResource imageResource;
	// public static String imagePath = "./resources/tmp/";
	public static String imagePath = "../../resources/tmp/";

	private ImageStoreProxy imageDao = new ImageStoreProxy(ImageStoreMethod.FTP);
	private ClientDAO clientDao = new ClientDAO();
	
	private ImageResource() {
		clientMap = new HashMap<Integer, ClientInfo>();
		clusterMap = new HashMap<Integer, ClientInfo>();
		serverList = ServerList.getInstance();
	}

	public static ImageResource getInstance() {
		if (imageResource == null)
			imageResource = new ImageResource();

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
	
	public void setLeader(){
		System.out.println(" *********** Node is set as leader **************");
		this.isLeader = true;
		/**/
	}

	@Override
	public void run() {
		while (true) {
			/*if (!forever && this.inbound.size() == 0)
				break;*/

			try {
				// block until a message is enqueued
			
				
				MgmtResponse mgmt = this.inbound.take();
				System.out.println("Is leader: " + mgmt.isLeader());
				if(mgmt.isLeader() == true){
					
					System.out.println("Is leader: " + isLeader);
					System.out.println("Cluster connected: " + !clustersConnected);
					if(isLeader && !clustersConnected){
						System.out.println("Is leader and connecting to cluster");
						createChannelForClustures();
						clustersConnected = true;
						
					}
				} else {
					Request imageResponse = null;

					try {
						imageResponse = getImageFromS3(mgmt);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (isLeader && mgmt.getDataSet().getClientId() != -1)
						sendImageToClusters(mgmt, imageResponse);

					sendImageToClients(mgmt, imageResponse);
				}
			}

			catch (InterruptedException ie) {
				break;
			} catch (Exception e) {

				break;
			}
		}

/*		if (!forever) {

		}*/
	}

	/* (non-Javadoc)
	 * @see poke.resources.ClientResource#process(poke.comm.Image.Request, poke.server.queue.PerChannelQueue)
	 */
	@Override
	public void process(Request request, PerChannelQueue channel) {

		boolean isClient = request.getHeader().getIsClient();
		if (isClient) {
			Integer clientId = request.getHeader().getClientId();
			if (!getClientMap().containsKey(clientId)) {
				addClient(clientId, new ClientInfo(channel, 0, false));
				//register the client or update its entry
				Long sentIndex = clientDao.updateClientEntry(this.nodeId, 
						String.valueOf(clientId), RaftManager.getInstance().getLastLogIndex());
				System.out.println("***************LAST INDEX******************"+sentIndex);
				if(sentIndex != RaftManager.getInstance().getLastLogIndex()) {
					List<MgmtResponse> list = RaftManager.getInstance().getDataSetFromIndex(sentIndex);
					for(MgmtResponse res : list) {
						try {
							Request imageResponse = getImageFromS3(res);
							this.sendImageToClients(res, imageResponse);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
			} else {
				getClientInfo(clientId).setChannel(channel);
			}
		}

		else {

			Integer clusterId = request.getHeader().getClusterId();
			if (!getClusterMap().containsKey(clusterId) ) {
				if(isLeader){
					System.out.println("Adding cluster to network: " + clusterId);
				addCluster(clusterId, new ClientInfo(channel, 0, true));
				channel.getOutbound().add(MessageBuilder.buildPingMessage());
				}
			} else {
				getClusterInfo(clusterId).setChannel(channel);
			}

		}
		if(!request.getPing().getIsPing()){
		boolean stored = storeImageInS3(request);
		if (stored) {
			Management mgmtMessage = MessageBuilder.buildMgmtMessage(request);
			ManagementQueue.enqueueRequest(mgmtMessage, null);
		}
		}

	}

	private boolean storeImageInS3(Request request) {
		byte[] byteImage = request.getPayload().getData().toByteArray();
		String key = request.getPayload().getReqId();
		InputStream in = new ByteArrayInputStream(byteImage);
		BufferedImage bImageFromConvert;

		System.out.println("********Image recieved********");
		try {
			File file = new File(imagePath, key + ".png");
			if (!file.exists()) {
				file.createNewFile();
			}
			bImageFromConvert = ImageIO.read(in);
			ImageIO.write(bImageFromConvert, "png", file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// logic for storing image in s3
		if (imageDao.uploadImage(key)) {
			return true;
		} else {
			return false;
		}
		// return true;
	}

	private Request getImageFromS3(MgmtResponse mgmt) throws IOException {
		String imageKey = mgmt.getDataSet().getDataSet().getValue();

		// call get image from DAO
		// DAO will load image to temp folder
		// fetching image file from temp folder.

		if (imageDao.getImage(imageKey)) {
			byte[] myByeImage;
			File image = new File(imagePath + imageKey + ".png");
			BufferedImage originalImage = ImageIO.read(image);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ImageIO.write(originalImage, "png", baos);
			baos.flush();
			myByeImage = baos.toByteArray();
			baos.close();
			ByteString bs = ByteString.copyFrom(myByeImage);
			image.delete();
			return MessageBuilder.buildRequestMessage(mgmt, bs);
		} else {
			throw new IOException();
		}
	}



	public void processRequestFromMgmt(List<MgmtResponse> list) {
		System.out.println("Putting msg in inbout - Imageresource");
		inbound.addAll(list);

	}

	private void sendImageToClients(MgmtResponse mgmt, Request imageResponse) {

		Iterator<Entry<Integer, ClientInfo>> iterator = clientMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<Integer, ClientInfo> entry = (Entry<Integer, ClientInfo>) iterator
					.next();
			if (entry.getKey() != mgmt.getDataSet().getClientId()
					&& entry.getValue().getLastSentIndex() < mgmt.getLogIndex()) {
				clientMap.get(entry.getKey()).getChannel().getOutbound()
						.add(imageResponse);
				//update lastsentindex
				
			}
			System.out.println("***************mgmt.getLogIndex()***************"+mgmt.getLogIndex());
			new ClientDAO().updateClientEntry(Integer.toString(conf.getNodeId()), Integer.toString(entry.getKey()), mgmt.getLogIndex());
		}
	}

	private void sendImageToClusters(MgmtResponse mgmt, Request imageResponse) {
		
		System.out.println("Sending image to clusters - cluster size" + clusterMap.size());
		Iterator<Entry<Integer, ClientInfo>> iterator = clusterMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			System.out.println("sending...");
			Entry<Integer, ClientInfo> entry = (Entry<Integer, ClientInfo>) iterator
					.next();
			Channel channel = clusterMap.get(entry.getKey()).getChannel().getChannel();
			if(channel != null && channel.isOpen() && channel.isWritable()) {
				System.out.println("sending..." + entry.getKey());
					clusterMap.get(entry.getKey()).getChannel().getOutbound()
					.add(imageResponse);
					//update lastsentindex		
				System.out.println("***************mgmt.getLogIndex()*************** "+mgmt.getLogIndex());
				//clientDao.updateClientEntry(this.nodeId, String.valueOf(entry.getKey()), mgmt.getLogIndex());
			}
			
			
		}
	}
	
	private void createChannelForClustures(){
		List<TCPAddress> nodes = this.serverList.addresses;
		System.out.println("Number of nodes: " + nodes.size());
		for(TCPAddress node: nodes){
			System.out.println("Sending ping to other cluster");
			ChannelCreator.getInstance().createChannelToNode(node);
			Channel channel = ChannelCreator.getInstance().allNodeChannels.get(node);
			ChannelFuture cf = channel.writeAndFlush(MessageBuilder.buildPingMessage()).syncUninterruptibly();
			if(cf.isDone())
			System.out.println("Cluster connections successful");
		}
	}
	
	public void setConf(ServerConf conf) {
		this.conf = conf;
		this.nodeId = String.valueOf(conf.getNodeId());
	}
	
	public ServerConf getConf(){
		return this.conf;
	}
}
