package poke.server.managers;

import java.beans.Beans;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.resources.data.MgmtResponse;
import poke.server.conf.ServerConf;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.Raft;
import poke.server.election.raft.state.LeaderState;

public class RaftManager implements ElectionListener {

	protected static Logger logger = LoggerFactory.getLogger("raftManager");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;

	/** The election that is in progress - only ONE! */
	private Election election;
	private Integer syncPt = 1;

	private RaftMonitor monitor = new RaftMonitor();

	private int timeOut = getRandomTimeOut();
	private int sHeartRate = (3 * timeOut) / 4;

	public static RaftManager initManager(ServerConf conf) {
		RaftManager.conf = conf;
		instance.compareAndSet(null, new RaftManager());

		return instance.get();
	}

	public static RaftManager getInstance() {
		// TODO throw exception if not initialized!
		return instance.get();
	}

	public Integer whoIsTheLeader() {
		return ((Raft) electionInstance()).getLeaderId();
	}

	/**
	 * Starts the monitor thread
	 */
	public void startMonitor() {
		monitor.start();
	}

	/**
	 * Process raftMessage and sends the response message
	 * @param mgmt
	 */
	public void processRequest(Management mgmt) {
		if (!mgmt.hasRaftMessage())
			return;
		
		if (mgmt.getHeader().getOriginator() != ((Raft) electionInstance())
				.getNodeId()) {
			Management rtn = electionInstance().process(mgmt);
			
			if (rtn != null) {
				// if toNode is present in header, send point-to-point msg
				if (rtn.getHeader().hasToNode()) {
					ConnectionManager.sendToNode(rtn.getHeader().getToNode(), rtn);

				} else {
					ConnectionManager.broadcastAndFlush(rtn);
				}
			}
		}
	}

	@Override
	public void concludeWith(boolean success, Integer LeaderID) {

	}

	/**
	 * Get timeOut period randomly chosen between
	 * min and max milliseconds
	 * @return
	 */
	private int getRandomTimeOut() {
		int max = 5000;
		int min = 4000;
		Random randTimeOut = new Random();
		return randTimeOut.nextInt((max - min) + 1) + min;
	}

	private Election electionInstance() {
		if (election == null) {
			synchronized (syncPt) {
				if (election != null)
					return election;

				// new election
				String clazz = RaftManager.conf.getElectionImplementation();

				// if an election instance already existed, this would
				// override the current election
				try {
					election = (Election) Beans.instantiate(this.getClass()
							.getClassLoader(), clazz);
					election.setNodeId(conf.getNodeId());
					election.setListener(this);

					// this sucks - bad coding here! should use configuration
					// properties
					if (election instanceof Raft) {
						logger.warn("Node " + conf.getNodeId()
								+ " starting Raft with total nodes "
								+ (HeartbeatManager.getInstance().outgoingHB.size() + 1));
						
						((Raft) election).setTotalNodes(HeartbeatManager.getInstance().outgoingHB.size() + 1);
					}

				} catch (Exception e) {
					logger.error("Failed to create " + clazz, e);
				}
			}
		}
		return election;
	}

	/**
	 * Monitor thread sends heartbeat messages to followers if
	 * currentState is Leader.
	 * 
	 * if timeOut period elapses without a heartbeat, initiate
	 * election
	 */
	public class RaftMonitor extends Thread {
		boolean forever = true;

		@Override
		public void run() {
			while (forever) {
				try {
					Thread.sleep(sHeartRate);
					updateNodeCount();
					if (((Raft) electionInstance()).getState() instanceof LeaderState) {
						sendAppendRequest();
					} else {
						long lastKnownBeat = ((Raft) electionInstance())
								.getLastKnownBeat();
						long now = System.currentTimeMillis();
						if ((now - lastKnownBeat) >= timeOut) {
							sendRequestVoteNotice();
						}
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					logger.error("Unexpected management communcation failure",
							e);
					break;
				}
			}
		}
	}

	/**
	 * Send requestVote notices to all other nodes
	 */
	private void sendRequestVoteNotice() {
		Management mgt = ((Raft) electionInstance()).getRequestVoteNotice();
		// if only one node in the network
		if(((Raft) electionInstance()).hasWonElection()) {
			mgt = ((Raft) electionInstance()).declareLeader();
		}
		ConnectionManager.broadcastAndFlush(mgt); 
	}

	/**
	 * Send appendRequest/Heartbeat to all followers - broadcast
	 */
	private void sendAppendRequest() {
		Management mgt = ((Raft) electionInstance()).getAppendRequest();
		ConnectionManager.broadcastAndFlush(mgt);
	}
	
	/**
	 * Send appendRequest to every node - point to point
	 * @param appendRequests
	 */
	public void sendAppendRequest(Map<Integer, Management> appendRequests) {
		for(Integer nodeId : appendRequests.keySet()) {
			if(appendRequests.get(nodeId) != null)
				ConnectionManager.sendToNode(nodeId, appendRequests.get(nodeId));
		}
	}
	
	/**
	 * Check if new nodes are connected in network. 
	 * If yes, update total node count 
	 */
	private void updateNodeCount() {
		int totalNodes = HeartbeatManager.getInstance().outgoingHB.size() + 1;
		if(totalNodes  != ((Raft) electionInstance()).getTotalNodes()) {
			((Raft) electionInstance()).setTotalNodes(totalNodes);
		}
	}
	
	/**
	 * Send a Management msg to specified nodeId
	 * @param nodeId
	 * @param msg
	 */
	public void sendMgmtRequest(Integer nodeId, Management msg) {
		if(msg != null) {
			ConnectionManager.sendToNode(nodeId, msg);
		}
	}
	
	/**
	 * Retrieve List of DataSets from log from startIndex to lastIndex
	 * @param startIndex
	 * @return
	 */
	public List<MgmtResponse> getDataSetFromIndex(long startIndex) {
		return ((Raft) electionInstance()).getDataSetFromLog(startIndex);
	}
	
	public Long getLastLogIndex(){
		return ((Raft) electionInstance()).getLog().getCommitIndex();
	}
	
	
}