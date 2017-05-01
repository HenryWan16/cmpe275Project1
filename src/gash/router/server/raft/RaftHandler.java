package gash.router.server.raft;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;
import java.util.regex.Pattern;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;


public class RaftHandler implements Runnable {

	private int dt;
	private int timebase = 5000;
	private volatile int timeout;
	private volatile long lastKnownBeat;
	
	private ServerState serverState;
	private int nodeId = -1;
	private int leaderNodeId = -1;
	private int currentNodeMode = 1; //1: Follower, 2: Candidate, 3:Leader

	private String host;
	private int port;
	private EdgeMonitor edgeMonitor;

	private long timerStart = 0;

	// This servers states
	private volatile NodeState nodeState;
	public NodeState leader;
	public NodeState candidate;
	public NodeState follower;

	public static RaftHandler instance;
	public Hashtable<String, String> logs = new Hashtable<String, String>();
	
	private Random rand = new Random();
	private int term = 0;

	public RaftHandler(ServerState state) {
		this.serverState = state;
		instance = this;
	}
	

	public static RaftHandler getInstance() {
		return instance;
	}
	
	public void init() {
		try {	
			leader = new LeaderNode(this);
			candidate = new CandidateNode(this);
			follower = new FollowerNode(this);
			
			host = "";
			try {
				Enumeration e = NetworkInterface.getNetworkInterfaces();
				String pattern1 = "169.254.*.*";
				String pattern2 = "192.168.*.*";
				while(e.hasMoreElements())
				{
					boolean isMatch1 = false, isMatch2 = false;
				    NetworkInterface n = (NetworkInterface) e.nextElement();
				    Enumeration ee = n.getInetAddresses();
				    while (ee.hasMoreElements())
				    {
				        InetAddress i = (InetAddress) ee.nextElement();
				        String ipAddress = i.getHostAddress();

				        isMatch1 = Pattern.matches(pattern1, ipAddress);
				        isMatch2 = Pattern.matches(pattern2, ipAddress);
				        
				        if (isMatch1 || isMatch2) {
				        	host = ipAddress;
				        	break;
				        }
				        else {
				        	host = "localhost";
				        }						        
				    }
				    if (isMatch1 || isMatch2) break;
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			port = serverState.getConf().getWorkPort();
			edgeMonitor = serverState.getEmon();

			lastKnownBeat = System.currentTimeMillis();
			dt = serverState.getConf().getHeartbeatDt();
			nodeId = serverState.getConf().getNodeId();

			setRandomTimeout();
			
			//initially all node will be a follower
			nodeState = follower;
			currentNodeMode = 1;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		System.out.println("Hearbeat initially is " + dt);
		System.out.println("Election timeout initially is " + timeout);
		
		//wait until get connected into the network
		try { Thread.sleep(5000); } catch (Exception e) {};

		while (true) {
			timerStart = System.currentTimeMillis();				
			nodeState.run();
		}

	}
	
	public synchronized void flushAllChannels() {
		for (EdgeInfo ei:this.edgeMonitor.getOutboundEdges().getMap().values()) {
			if (ei.isActive() && ei.getChannel().isActive()) {
				ei.getChannel().flush();
			}
		}
	}

	public synchronized EdgeMonitor getEdgeMonitor() {
		return this.edgeMonitor;
	}
	public synchronized ServerState getServerState() { return this.serverState; }
	
	public synchronized int getTimebase() {
		return timebase;
	}
	
	public synchronized void setRandomTimeout() {
		timeout = rand.nextInt(dt) + timebase;
	}
	
	public synchronized void setTimeout(int t) {
		timeout = t;
	}
	
	public synchronized int getTimeout() {
		return this.timeout;
	}

	public synchronized long getDt() {
		return this.dt;
	}
	public synchronized long getLastKnownBeat() {
		return lastKnownBeat;
	}

	public synchronized void setLastKnownBeat(long t) {
		lastKnownBeat = t;
	}
	public synchronized long getTimerStart() {
		return timerStart;
	}

	public synchronized void setTimerStart(long t) {
		this.timerStart = t;
	}

	public synchronized int getNodeId() {
		return this.nodeId;
	}

	public synchronized int getPort() {
		return this.port;
	}

	public synchronized String getHost() {
		return this.host;
	}

	public synchronized void setNodeState(NodeState state, int mode) {
		this.nodeState = state;
		this.currentNodeMode = mode;
	}

	public synchronized NodeState getNodeState() {
		return this.nodeState;
	}

	public synchronized int getNodeMode() {
		return this.currentNodeMode;
	}
	
	public synchronized void increaseTerm() {
		this.term += 1; 
	}
	
	public synchronized void decreaseTerm() {
		this.term -= 1; 
	}
	
	public synchronized void setTerm(int term) {
		this.term = term;
	}

	public synchronized int getTerm() {
		return this.term;
	}

	public synchronized void setLeaderNodeId(int id) {
		this.leaderNodeId = id;
		this.serverState.setLeaderId(id);
	}
	
	public synchronized int getLeaderNodeId() {
		return this.leaderNodeId;
	}
}
