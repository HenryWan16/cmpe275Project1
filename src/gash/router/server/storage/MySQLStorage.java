package gash.router.server.storage;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by henrywan16 on 4/13/17.
 */
public class MySQLStorage implements FileStorage {
    private FileChunk fileChunk;

    protected static Logger logger = LoggerFactory.getLogger("database");

    public static final String sDriver = "jdbc.driver";
    public static final String sUrl = "jdbc.url";
    public static final String sUser = "jdbc.user";
    public static final String sPass = "jdbc.password";

    protected Properties cfg;
    protected BoneCP cpool;

    public MySQLStorage(FileChunk fileChunk, Properties cfg) {
        init(cfg);
        this.fileChunk = fileChunk;
    }

    public MySQLStorage(String fileName, int chunkId, String file_id, byte[] data, int totalNoOfChunks, Properties cfg) {
        init(cfg);
        this.fileChunk = new FileChunk(fileName, chunkId, file_id, data, totalNoOfChunks);
    }

    public MySQLStorage(String fileName, int chunkId, byte[] data, int totalNoOfChunks, Properties cfg) {
        init(cfg);
        this.fileChunk = new FileChunk(fileName, chunkId, data, totalNoOfChunks);
    }

    public MySQLStorage(String fileName, int chunkId, byte[] data, Properties cfg) {
        init(cfg);
        this.fileChunk = new FileChunk(fileName, chunkId, data, 1);
    }

    public MySQLStorage(String fileName, int chunkId, Properties cfg) {
        init(cfg);
        String data = "";
        byte[] dataArray = data.getBytes();
        this.fileChunk = new FileChunk(fileName, chunkId, dataArray,1);
    }

    public MySQLStorage(Properties cfg) {
        init(cfg);
    }

    public void init(Properties cfg) {
        if (cpool != null)
            return;

        this.cfg = cfg;

        try {
            Class.forName(cfg.getProperty(sDriver));
            BoneCPConfig config = new BoneCPConfig();
            config.setJdbcUrl(cfg.getProperty(sUrl));
            config.setUsername(cfg.getProperty(sUser, "sa"));
            config.setPassword(cfg.getProperty(sPass, ""));
            config.setMinConnectionsPerPartition(5);
            config.setMaxConnectionsPerPartition(10);
            config.setPartitionCount(1);

            cpool = new BoneCP(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * (non-Javadoc)
     * @see gash.jdbc.repo.Repository#release()
     */
    public void release() {
        if (cpool == null)
            return;

        cpool.shutdown();
        cpool = null;
    }

    public boolean createTable() {
        Connection conn = null;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                logger.info("FileDB Connection successful!");
                Statement stmt = conn.createStatement();
                String createTable = "CREATE TABLE FileChunk\n" +
                        "(\n" +
                        "fileName varchar(255),\n" +
                        "chunkID int,\n" +
                        "data varbinary(8388608),\n" +
                        "file_id varchar(255),\n" +
                        "totalNoOfChunks int,\n" +
                        "Primary Key(fileName, chuckID),\n" +
                        ");";
                boolean createResult = stmt.execute(createTable);
                if (createResult == false) {
                    logger.info("Create table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Create table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on creating chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean dropTable() {
        Connection conn = null;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                logger.info("FileDB Connection successful!");
                Statement stmt = conn.createStatement();
                String dropTable = "DROP TABLE FileChunk;";
                boolean dropResult = stmt.execute(dropTable);
                if (dropResult == false) {
                    logger.info("Drop table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Drop table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on dropping chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean insertRecordFileChunk() {
        if (this.fileChunk == null) {
            logger.info("No fileChunk to insert.");
            return false;
        }
        Connection conn = null;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String insertRecord = "INSERT INTO FileChunk (fileName, chunkID, data, file_id, totalNoOfChunks)\n" +
                        "VALUES ('" + this.fileChunk.getFileName() + "'," + this.fileChunk.getChunkId() + ",'" + this.fileChunk.getData() + "','" + this.fileChunk.getFile_id() + "'," + this.fileChunk.getTotalNoOfChunks() + ");";
                boolean insertResult = stmt.execute(insertRecord);
                if (insertResult == false) {
                    logger.info("Insert a new record to Table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Insert table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on inserting a record: chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean deleteRecordFileChunk() {
        Connection conn = null;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String deleteRecord = "DELETE FROM FileChunk\n" +
                        "WHERE fileName='" + this.fileChunk.getFileName() + "' and chunkID=" + this.fileChunk.getChunkId() + ";";
                boolean deleteResult = stmt.execute(deleteRecord);
                if (deleteResult == false) {
                    logger.info("Delete a new record from Table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Delete table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on deleting a record: chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public int updateRecordFileChunk() {
        Connection conn = null;
        int result = 0;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String updateRecord = "UPDATE FileChunk\n" +
                        "SET fileName='" + this.fileChunk.getFileName() + "', chunkID='" + this.fileChunk.getChunkId() + "', data='" + this.fileChunk.getData() + "', totalNoOfChunks='" + this.fileChunk.getData() + "', file_id='" + this.fileChunk.getFile_id() + "\n" +
                        "WHERE fileName='" + this.fileChunk.getFileName() + "' and chunkID=" + this.fileChunk.getChunkId() + ";";
                result = stmt.executeUpdate(updateRecord);
                if (result == 0) {
                    logger.info("No record in Table FileChunk in the FileDB updated. ");
                }
                else {
                    logger.info("Update table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on updating a record: chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return -1;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public ResultSet selectRecordFileChunk() {
        if (this.fileChunk == null) {
            logger.info("FileChunk is null when select from FileChunk Table.");
            return null;
        }
        Connection conn = null;
        try {
            conn = cpool.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String selectRecord = "SELECT * FROM FileChunk\n" +
                        "WHERE fileName='" + this.fileChunk.getFileName() + "' and chunkID=" + this.fileChunk.getChunkId() + ";";
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                }
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: chunkId " + this.fileChunk.getChunkId() + " of the file " + this.fileChunk.getFileName(), ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public FileChunk getFileChunk() {
        return fileChunk;
    }

    public void setFileChunk(FileChunk fileChunk) {
        this.fileChunk = fileChunk;
    }

    //    optional int32 chunkId = 1;
//    optional bytes data = 2;
//    required string filename = 3;
//    optional int32 totalNoOfChunks = 4;  //total number of chunks of a requested file
//    optional string file_id = 5; // can be a message digest of file. Will also serve as file validation code

    private class FileChunk {
        private String fileName;
        private int chunkId;
        private byte[] data;
        private int totalNoOfChunks;
        private String file_id; //optional default is "0";

        public FileChunk(String fileName, int chunkId, byte[] data, int totalNoOfChunks) {
            this.file_id = "0";
            this.chunkId = chunkId;
            this.fileName = fileName;
            this.data = data;
            this.totalNoOfChunks = totalNoOfChunks;
        }

        public FileChunk(String fileName, int chunkId, String file_id, byte[] data, int totalNoOfChunks) {
            this.file_id = file_id;
            this.chunkId = chunkId;
            this.fileName = fileName;
            this.data = data;
            this.totalNoOfChunks = totalNoOfChunks;
        }

        public String getFile_id() {
            return file_id;
        }

        public void setFile_id(String file_id) {
            this.file_id = file_id;
        }

        public int getChunkId() {
            return chunkId;
        }

        public void setChunkId(int chunkId) {
            this.chunkId = chunkId;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
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
    }
}
