package gash.router.server;

import java.util.Hashtable;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.raft.RaftHandler;
import gash.router.server.tasks.TaskList;
import io.netty.channel.Channel;

public class ServerState {
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	private RaftHandler handler;
	private int leaderId;
	private String status = "";
	public static Channel nextCluster = null;
	public static Hashtable<Integer, Hashtable<Channel, Integer>> channelsTable = new Hashtable<Integer, Hashtable<Channel, Integer>>();

	public String getStatus() { 
		return status;
    }

	public void setStatus(String status) { 
		this.status = status;
    }

	public RoutingConf getConf() {
		return conf;
	}
	
	public int getLeaderId() {
	    return leaderId;
	}
	
	public void setLeaderId(int id) {
	    leaderId = id;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public EdgeMonitor getEmon() {
		return emon;
	}

	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}
	
	public void setHandler(RaftHandler handler) {
		this.handler = handler;
	}
	
	public RaftHandler getHandler() {
		return this.handler;
	}

}
