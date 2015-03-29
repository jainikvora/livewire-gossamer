package poke.server.managers;

import java.beans.Beans;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.Management;
import poke.server.conf.ServerConf;
import poke.server.election.Election;
import poke.server.election.ElectionListener;
import poke.server.election.raft.Raft;


public class RaftManager implements ElectionListener{
	
	protected static Logger logger = LoggerFactory.getLogger("election");
	protected static AtomicReference<RaftManager> instance = new AtomicReference<RaftManager>();

	private static ServerConf conf;
	
	/** The election that is in progress - only ONE! */
	private Election election;
	//private int electionCycle = -1;
	private Integer syncPt = 1;

	/** The leader */
	Integer leaderNode;
	
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
		if (!mgmt.hasElection())
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

		Management rtn = electionInstance().process(mgmt);
		if (rtn != null)
			ConnectionManager.broadcast(rtn);
	}
	
	/*public void assessCurrentState(Management mgmt) {
		// logger.info("ElectionManager.assessCurrentState() checking elected leader status");

		if (firstTime > 0 && ConnectionManager.getNumMgmtConnections() > 0) {
			// give it two tries to get the leader
			this.firstTime--;
			askWhoIsTheLeader();
		} else if (leaderNode == null && (election == null || !election.isElectionInprogress())) {
			// if this is not an election state, we need to assess the H&S of
			// the network's leader
			synchronized (syncPt) {
				startElection();
			}
		}
	}*/
	
	@Override
	public void concludeWith(boolean success, Integer LeaderID) {
		// 
		if (success) {
			/*logger.info("----> the leader is " + leaderID);
			this.leaderNode = leaderID;*/
		}

		election.clear();
	}
	
	/*private void respondToWhoIsTheLeader(Management mgmt) {
		if (this.leaderNode == null) {
			logger.info("----> I cannot respond to who the leader is! I don't know!");
			return;
		}

		logger.info("Node " + conf.getNodeId() + " is replying to " + mgmt.getHeader().getOriginator()
				+ "'s request who the leader is. Its Node " + this.leaderNode);

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);

		LeaderElection.Builder elb = LeaderElection.newBuilder();
		elb.setElectId(electionCycle);
		elb.setAction(ElectAction.THELEADERIS);
		elb.setDesc("Node " + this.leaderNode + " is the leader");
		elb.setCandidateId(this.leaderNode);
		elb.setExpires(-1);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		// now send it to the requester
		logger.info("Election started by node " + conf.getNodeId());
		try {

			ConnectionManager.getConnection(mgmt.getHeader().getOriginator(), true).write(mb.build());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void askWhoIsTheLeader() {
		logger.info("Node " + conf.getNodeId() + " is searching for the leader");

		MgmtHeader.Builder mhb = MgmtHeader.newBuilder();
		mhb.setOriginator(conf.getNodeId());
		mhb.setTime(System.currentTimeMillis());
		mhb.setSecurityCode(-999); // TODO add security

		VectorClock.Builder rpb = VectorClock.newBuilder();
		rpb.setNodeId(conf.getNodeId());
		rpb.setTime(mhb.getTime());
		rpb.setVersion(electionCycle);
		mhb.addPath(rpb);

		LeaderElection.Builder elb = LeaderElection.newBuilder();
		elb.setElectId(-1);
		elb.setAction(ElectAction.WHOISTHELEADER);
		elb.setDesc("Node " + this.leaderNode + " is asking who the leader is");
		elb.setCandidateId(-1);
		elb.setExpires(-1);

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setElection(elb.build());

		// now send it to the requester
		ConnectionManager.broadcast(mb.build());
	}*/

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
					((Raft) election).setTotalNodes(conf.getAdjacent().getAdjacentNodes().size());
					election.setListener(this);

					// this sucks - bad coding here! should use configuration
					// properties
					if (election instanceof Raft) {
						logger.warn("Node " + conf.getNodeId() + " setting max hops to arbitrary value (4)");
						//((FloodMaxElection) election).setMaxHops(4);
						
					}

				} catch (Exception e) {
					logger.error("Failed to create " + clazz, e);
				}
			}
		}

		return election;

	}

}
