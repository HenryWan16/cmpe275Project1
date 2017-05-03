package gash.router.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;


public class MergeWorker implements Runnable{
	protected static Logger logger = LoggerFactory.getLogger("MergeWorker");
    private int totalNoOfChunks;
    private static HashMap<Integer,byte[]> chunkIdDataMap;
    private static HashSet<Integer> chunkIdSet;
    public int num;
    protected static MergeWorker mergeWorker;
    public boolean successMerge = false;
    public static final Object usageLock = new Object();
    private int currentChunkId = 0;
    byte[] file = new byte[0];
    String filename;

    public static MergeWorker getMergeWorkerInstance(){
        if(mergeWorker == null)
            mergeWorker = new MergeWorker();
        return mergeWorker;
    }

    public MergeWorker() {
        mergeWorker = this;
//        this.totalNoOfChunks = chunkNum;
        chunkIdDataMap = new HashMap<Integer, byte[]>();
        chunkIdSet = new HashSet<Integer>();
        num = 0;
    }

    /**
     * listen to table update and merge when the current chunk is in the table
     */
    @Override
    public void run() {
        while(!successMerge){
            //receive chunk from inbound queue
            if(chunkIdDataMap.containsKey(currentChunkId)){
                byte[] temp = file;
                byte[] add = chunkIdDataMap.get(currentChunkId);
                file = new byte[temp.length + add.length];
                System.arraycopy(temp, 0, file, 0, temp.length);
                System.arraycopy(add, 0, file, temp.length, add.length);
                chunkIdDataMap.remove(currentChunkId);
                chunkIdSet.add(currentChunkId);
                currentChunkId++;
            }

            if(currentChunkId != 0 && currentChunkId == totalNoOfChunks)
                successMerge = true;
            
            try { Thread.sleep(200); } catch (Exception e){ e.printStackTrace();}
        }
        getFile(file, filename);
    }

    public void setTotalNoOfChunks(int num) {
        totalNoOfChunks = num;
    }

    /**
     * update the table of chunks with new chunks
     * @param chunk
     */
    public static void upDateTable(Common.Chunk chunk) {
        int id = chunk.getChunkId();
        byte[] data = chunk.getChunkData().toByteArray();
        System.out.println("MergeWork upDateTable id = " + id);
        synchronized (usageLock) {
            if(!chunkIdSet.contains(id))
                chunkIdDataMap.put(id, data);
        }
        System.out.println("table update complete for current chunk");
        
    }
    
    /**
     * return new file generated from chunks in the table
     * @param bytefile
     * @param filename
     */
    public void getFile(byte[] bytefile, String filename){
    	

		File theDir = new File("downloads");

		// if the directory does not exist, create it
		if (!theDir.exists()) {
		    System.out.println("creating directory: " + theDir.getName());
		    boolean result = false;

		    try{
		        theDir.mkdir();
		        result = true;
		    } 
		    catch(SecurityException se){
		        System.out.println("Not able to write in current directory");
		    }        
		    if(result) {    
		        System.out.println("downloads directory created");  
		    }
		}
    	
    	
        System.out.println("Merge complete, return the file into ./downloads directory");

        File newFile = new File(theDir, filename);
        try {
            newFile.canWrite();
            FileOutputStream fos = new FileOutputStream(newFile);
            fos.write(bytefile);
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void setResultFileName(String fname){
        this.filename = fname;
    }
}
