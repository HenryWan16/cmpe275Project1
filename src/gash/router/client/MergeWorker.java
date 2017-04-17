package gash.router.client;

import com.google.protobuf.ByteString;
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
    private int currentChunkId = 0;
    byte[] file;
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
            if(currentChunkId == totalNoOfChunks)
                successMerge = true;
        }
        getFile(file, filename);

    }

    public void setTotalNoOfChunks(int num) {
        totalNoOfChunks = num;
    }

    public static void upDateTable(Common.Chunk chunk) {
        int id = chunk.getChunkId();
        byte[] data = chunk.getChunkData().toByteArray();
        synchronized (usageLock) {
            if(!chunkIdSet.contains(id))
                chunkIdDataMap.put(id, data);
        }
    }

    public void getFile(byte[] bytefile, String filename){
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

    public void getFileName(String fname){
        this.filename = fname;
    }
}
