package gash.router.server.storage;

import java.util.ArrayList;

/**
 * Created by henrywan16 on 4/16/17.
 */
public class TestSQLOperations {
    private MySQLStorage mySQLStorage;

    public TestSQLOperations() {
        this.mySQLStorage = new MySQLStorage();
    }

    public void createTable() throws Exception {
        String temp = "Hello every.\n" +
                "What can I do for you?\n" +
                "\n" +
                "Test creating table.";
        String fileName = "test.txt";
        int chunkID = 2;
        byte[] data = temp.getBytes();
        int totalNoOfChunks = 3;
        String file_id = "Just for testing.";
        mySQLStorage.createTable();
        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
        ClassFileChunkRecord result = mySQLStorage.selectRecordFileChunk(fileName, chunkID);
        System.out.println("Selecting results: " + result);
        //mySQLStorage.deleteRecordFileChunk(fileName, chunkID);
    }

    public void dropTable() throws Exception {
        mySQLStorage.dropTable();
//        String temp = "Hello every.\n" +
//                "What can I do for you?\n" +
//                "\n" +
//                "Test drop table.";
//        String fileName = "test.txt";
//        int chunkID = 2;
//        byte[] data = temp.getBytes();
//        int totalNoOfChunks = 10;
//        String file_id = "Just for testing.";
//        mySQLStorage.createTable();
//        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
//        ResultSet rs = mySQLStorage.selectRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
//        while(rs.next())
//            System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3));
    }

    public void insertRecordFileChunk() throws Exception {
        String temp = "Hello every.\n" +
                "How are you?\n" +
                "\n" +
                "Test drop table.";
        String fileName = "files/input.txt";
        int chunkID = 1;
        byte[] data = temp.getBytes();
        int totalNoOfChunks = 3;
        String file_id = "Just for testing.";
        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);

        temp = "Hello World";
        chunkID = 2;
        data = temp.getBytes();
        file_id = "Hello World.";
        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);

        temp = "How old are you?";
        chunkID = 3;
        data = temp.getBytes();
        file_id = "Age.";
        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
    }
    
    public void insertRecordFileChunk(int chunkId) throws Exception {
        String temp = "Hello every.\n" +
                "How are you?\n" +
                "\n" +
                "Test drop table.";
        String fileName = "files/input.txt";
        int chunkID = chunkId;
        byte[] data = temp.getBytes();
        int totalNoOfChunks = 10;
        String file_id = "Just for testing.";
        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);

//        temp = "Hello World";
//        chunkID = 1;
//        data = temp.getBytes();
//        file_id = "Hello World.";
//        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
//
//        temp = "How old are you?";
//        chunkID = 3;
//        data = temp.getBytes();
//        file_id = "Age.";
//        mySQLStorage.insertRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
    }

    public void deleteRecordFileChunk() throws Exception {
        String fileName = "test.txt";
        int chunkID = 1;
       // mySQLStorage.deleteRecordFileChunk(fileName, chunkID);
    }

    public void updateRecordFileChunk() throws Exception {
        String temp = "We try to modify.\n" +
                "all the byte[] data \n" +
                "\n" +
                "in chunk 0.";
        String fileName = "test.txt";
        int chunkID = 0;
        byte[] data = temp.getBytes();
        int totalNoOfChunks = 10;
        String file_id = "Finished updating test.";
        mySQLStorage.updateRecordFileChunk(fileName, chunkID, data, totalNoOfChunks, file_id);
    }

    public void selectRecordFileChunk() throws Exception {
        String fileName = "test.txt";
        int chunkID = 0;
        ClassFileChunkRecord result = mySQLStorage.selectRecordFileChunk(fileName, chunkID);
        System.out.println("Selecting results: " + result);
    }

    public void tearDown() throws Exception {
        this.mySQLStorage.dropTable();
    }
    
    public void checkFileExist(String fname) {
    	boolean answer = this.mySQLStorage.checkFileExist("files/test.txt");
    	System.out.println("checkFileExist(\"" + fname + "\") = " + answer);
    }
}
