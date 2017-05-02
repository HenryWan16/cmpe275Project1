package gash.router.server.storage;

import java.util.Arrays;


public class ClassFileChunkRecord {
    private String fileName;
    private int chunkID;
    private byte[] data;
    private int totalNoOfChunks;
    private String file_id;

    public ClassFileChunkRecord(String fileName, int chunkID, byte[] data, int totalNoOfChunks, String file_id) {
        this.fileName = fileName;
        this.chunkID = chunkID;
        this.data = data;
        this.totalNoOfChunks = totalNoOfChunks;
        this.file_id = file_id;
    }

    public ClassFileChunkRecord(String fileName, int chunkID, byte[] data, int totalNoOfChunks) {
        this.fileName = fileName;
        this.chunkID = chunkID;
        this.data = data;
        this.totalNoOfChunks = totalNoOfChunks;
    }

    public ClassFileChunkRecord(String fileName, int chunkID, byte[] data) {
        this.fileName = fileName;
        this.chunkID = chunkID;
        this.data = data;
    }

    public ClassFileChunkRecord(String fileName, int chunkID) {
        this.fileName = fileName;
        this.chunkID = chunkID;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getChunkID() {
        return chunkID;
    }

    public void setChunkID(int chunkID) {
        this.chunkID = chunkID;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getTotalNoOfChunks() {
        return totalNoOfChunks;
    }

    public void setTotalNoOfChunks(int totalNoOfChunks) {
        this.totalNoOfChunks = totalNoOfChunks;
    }

    public String getFile_id() {
        return file_id;
    }

    public void setFile_id(String file_id) {
        this.file_id = file_id;
    }

    @Override
    public String toString() {
        return "FileChunk {" +
                "fileName='" + fileName + '\'' +
                ", chunkID=" + chunkID +
                ", data=" + Arrays.toString(data) +
                ", totalNoOfChunks=" + totalNoOfChunks +
                ", file_id='" + file_id + '\'' +
                '}';
    }
}
