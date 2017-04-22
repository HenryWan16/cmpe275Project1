package gash.router.client;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import pipe.common.Common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by sam on 4/15/17.
 */
public class MergeWorker implements Runnable{

    private int totalNoOfChunks;
    private static HashMap<Integer,byte[]> chunkIdDataMap;
    private static HashSet<Integer> chunkIdSet;
    public int num;
    protected static MergeWorker mergeWorker;
    private boolean successMerge = false;
    public static final Object usageLock = new Object();
    private int currentChunkId = 1;
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
        System.out.println("successMerge = " + successMerge);
        System.out.println("current chunk id: "+currentChunkId);
        System.out.println("total number of chunks: "+totalNoOfChunks);
        System.out.println("table size now: "+chunkIdDataMap.size());
        while(!successMerge){
            System.out.println("table size now: "+chunkIdDataMap.size());
            System.out.println("current chunk id: "+currentChunkId);
            //receive chunk from inbound queue
            if(chunkIdDataMap.containsKey(currentChunkId)){
                System.out.println("start merge for currentchunkid: "+currentChunkId);
                byte[] temp = file;
                byte[] add = chunkIdDataMap.get(currentChunkId);
                file = new byte[temp.length + add.length];
                System.arraycopy(temp, 0, file, 0, temp.length);
                System.arraycopy(add, 0, file, temp.length, add.length);
                chunkIdDataMap.remove(currentChunkId);
                chunkIdSet.add(currentChunkId);
                currentChunkId++;
                System.out.println("merge part complete for part: "+currentChunkId);
            }
            if(currentChunkId == totalNoOfChunks+1)
                successMerge = true;
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
        synchronized (usageLock) {
            if(!chunkIdSet.contains(id))
            	System.out.println("Put chunk" + id + " to the MergeWorker Table.");
            	System.out.println("%%%%%%%%%%%%%%%%%%%%%%%" + "Chunk" + id + " data: " + new String(data));
                chunkIdDataMap.put(id, data);
                System.out.println("chunkIdDataMap.size = " + chunkIdDataMap.size());
        }
        System.out.println("table update complete for current chunk");
    }

    /**
     * return new file generated from chunks in the table
     * @param bytefile
     * @param filename
     */
    public void getFile(byte[] bytefile, String filename){
        System.out.println("merge complete, return the file to current directory");
        File newFile = new File(filename);
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
