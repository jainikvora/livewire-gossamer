package poke.server.managers;

import java.beans.Beans;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import poke.core.Mgmt.Management;
import poke.server.conf.ServerConf;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.Raft;
import poke.server.election.raft.Raft.RaftState;


public class RaftManager implements ElectionListener{
	
	protected static Logger logger = LoggerFactory.getLogger("Raft Manager");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;
	
	/** The election that is in progress - only ONE! */
	private Election election;
	//private int electionCycle = -1;
	private Integer syncPt = 1;

	/** The leader */
	Integer leaderNode;
	
	private RaftMonitor monitor =  new RaftMonitor();
	
	private int timeOut = getRandomTimeOut();
	private int sHeartRate = (3* timeOut) / 4;
	
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
		return this.leaderNode;
	}
	
	public void startMonitor() {
		monitor.start();
	}
	
	public void startElection() {
		/*electionCycle = electionInstance().createElectionID();
		
		LeaderElection.Builder elb = LeaderElection.newBuilder();
		elb.setElectId(electionCycle);
		elb.setAction(ElectAction.DECLAREELECTION);
		elb.setDesc("Node " + conf.getNodeId() + " detects no leader. Election!");
		elb.setCandidateId(conf.getNodeId()); // promote self
		elb.setExpires(2 * 60 * 1000 + System.currentTimeMillis()); // 1 minute

		// bias the voting with my number of votes (for algos that use vote
		// counting)

		// TODO use voting int votes = conf.getNumberOfElectionVotes();

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		// now send it out to all my edges
		logger.info("Election started by node " + conf.getNodeId());
		ConnectionManager.broadcast(mb.build());*/
	}

	public void processRequest(Management mgmt) {
		if (!mgmt.hasRaftMessage())
			return;

		//LeaderElection req = mgmt.getElection();

		// when a new node joins the network it will want to know who the leader
		// is - we kind of ram this request-response in the process request
		// though there has to be a better place for it
		/*if (req.getAction().getNumber() == LeaderElection.ElectAction.WHOISTHELEADER_VALUE) {
			respondToWhoIsTheLeader(mgmt);
			return;
		} else if (req.getAction().getNumber() == LeaderElection.ElectAction.THELEADERIS_VALUE) {
			logger.info("Node " + conf.getNodeId() + " got an answer on who the leader is. Its Node "
					+ req.getCandidateId());
			this.leaderNode = req.getCandidateId();
			return;
		}

		// else fall through to an election

		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// ran out of time so the election is over
				election.clear();
				return;
			}
		}*/
		System.out.println("Raft Manager");
		Management rtn = electionInstance().process(mgmt);
		if (rtn != null) {
			if(rtn.getHeader().hasToNode()) {
				ConnectionManager.getConnection(rtn.getHeader().getToNode(), true).writeAndFlush(mgmt);
			} else {
				ConnectionManager.broadcast(rtn);
			}
		}
			
	}
	
	@Override
	public void concludeWith(boolean success, Integer LeaderID) {
		// 
		if (success) {
			/*logger.info("----> the leader is " + leaderID);
			this.leaderNode = leaderID;*/
		}

		election.clear();
	}
	
	private int getRandomTimeOut() {
		int max = 5000;
		int min = 4000;
		Random randTimeOut = new Random();
		return randTimeOut.nextInt((max - min) + 1) + min;
	}

	private Election electionInstance() {
		if (election == null) {
			synchronized (syncPt) {
				if (election !=null)
					return election;
				
				// new election
				String clazz = RaftManager.conf.getElectionImplementation();

				// if an election instance already existed, this would
				// override the current election
				try {
					election = (Election) Beans.instantiate(this.getClass().getClassLoader(), clazz);
					election.setNodeId(conf.getNodeId());
					election.setListener(this);

					// this sucks - bad coding here! should use configuration
					// properties
					if (election instanceof Raft) {
						logger.warn("Node " + conf.getNodeId() + " starting Raft with total nodes " + conf.getAdjacent().getAdjacentNodes().size());
						//((FloodMaxElection) election).setMaxHops(4);
						((Raft) election).setTotalNodes(conf.getAdjacent().getAdjacentNodes().size() + 1);	
					}

				} catch (Exception e) {
					logger.error("Failed to create " + clazz, e);
				}
			}
		}
		return election;
	}
	
	public class RaftMonitor extends Thread {
		boolean forever = true;
		
		public RaftMonitor() {
			
		}
		
		@Override
		public void run() {
			while(forever) {
				try {
					Thread.sleep(sHeartRate);
					
					if(((Raft)electionInstance()).getCurrentState() == Raft.RaftState.Leader) {
						sendAppendRequest();
					} else {
						long lastKnownBeat = ((Raft)electionInstance()).getLastKnownBeat();
						long now = System.currentTimeMillis();
						if((now - lastKnownBeat) >= timeOut) {
							sendRequestVoteNotice();
						}
					}
					
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					logger.error("Unexpected management communcation failure", e);
					break;
				}
			}	
		}
	}
	
	private void sendRequestVoteNotice() {
		Management mgt = ((Raft)electionInstance()).getRequestVoteNotice();
		ConnectionManager.broadcast(mgt);
	}
		
	private void sendAppendRequest() {
		Management mgt = ((Raft)electionInstance()).getAppendRequest();
		ConnectionManager.broadcast(mgt);
	}
}
