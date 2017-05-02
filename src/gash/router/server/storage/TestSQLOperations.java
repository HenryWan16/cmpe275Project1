package gash.router.server.storage;


public class TestSQLOperations {
    private MySQLStorage mySQLStorage;

    public TestSQLOperations() {
        this.mySQLStorage = new MySQLStorage();
    }

    public void createTable() throws Exception {
        mySQLStorage.createTable();
    }

    public void dropTable() throws Exception {
        mySQLStorage.dropTable();
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
    	boolean answer = this.mySQLStorage.checkFileExist("files/simplefile.txt");
    	System.out.println("checkFileExist(\"" + fname + "\") = " + answer);
    }
}
