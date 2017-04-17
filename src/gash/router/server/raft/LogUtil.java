package gash.router.server.raft;

import java.util.ArrayList;
import java.util.Hashtable;

public class LogUtil {
	public static Hashtable<String, String> logs = RaftHandler.getInstance().logs;
	
	public static Hashtable<Integer, String> getListNodesToReadFile(String fname) {
		//will return as chunkId, location
		Hashtable<Integer, String> location = new Hashtable<Integer, String>();
		System.out.println("*****FileName********" + fname);
		for(String sKey: logs.keySet()) {
			System.out.println("*******sKey******" + sKey);
			if (sKey != null && sKey.contains(fname)) {
				String[] parts = sKey.split(";");
				int chunkSize = Integer.parseInt(parts[2]);
				int chunkId = Integer.parseInt(parts[1]);
				
				if (location.size() < chunkSize && !location.containsKey(chunkId)) {
					location.put(chunkId, logs.get(sKey));
				}
			}
		}
		System.out.println("*******at the end******");
		if (location.size() == 0) return null;
		return location;
	}
}
