package gash.router.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;


public class MySQLStorage {
    protected static Logger logger = LoggerFactory.getLogger("MySQL");

    public static Connection conn;
    public static MySQLStorage instance;
    

    public MySQLStorage() {
        init();
        instance = this;
    }
    
    public static MySQLStorage getInstance() {
        if (instance == null) {
            instance = new MySQLStorage();
        }
        return instance;
    }

    public void init() {

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/FileDB?&useSSL=true", RoutingConf.mySQLUser, RoutingConf.mySQLPwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        logger.info("Connecting to MySQL successed.");
        if (conn != null) {
            logger.info("FileDB Connection successful!");
        }
    }


    public boolean createTable() {
        
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                logger.info("FileDB Connection successful!");
                Statement stmt = conn.createStatement();
                String createTable = "CREATE TABLE FileChunk\n" +
                        "(\n" +
                        "fileName varchar(255),\n" +
                        "chunkID int,\n" +
                        "data longblob,\n" +
                        "file_id varchar(255),\n" +
                        "totalNoOfChunks int,\n" +
                        "Primary Key(fileName, chunkID)\n" +
                        ");";
                boolean createResult = stmt.execute(createTable);
                if (createResult == false) {
                    logger.info("Create table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Create table FileChunk in the FileDB successfully. ");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on creating Table FileChunk of the FileDB.", ex);
            try {
                conn.rollback();
            } catch (SQLException e) { }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                   // conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean dropTable() {

        try {
            // TODO complete code to use JDBC
            if (conn != null){
                logger.info("FileDB Connection successful!");
                Statement stmt = conn.createStatement();
                String dropTable = "DROP TABLE FileChunk;";
                boolean dropResult = stmt.execute(dropTable);
                if (dropResult == false) {
                    logger.info("Drop table FileChunk in the FileDB failed. ");
                } else {
                    logger.info("Drop table FileChunk in the FileDB successfully. ");
                }
            }
            // release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on dropping Table FileChunk from the FileDB. ", ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                 //   conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean insertRecordFileChunk(String fileName, int chunkID, byte[] data, int totalNoOfChunks, String file_id) {

        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to insert.");
            return false;
        }
        try {
            InputStream blob = null; 
            // TODO complete code to use JDBC
            if (conn != null) {
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                if (data != null) {
                    blob = new ByteArrayInputStream(data);
                }
                String insertRecord = "INSERT INTO FileChunk VALUES (?, ?, ?, ?, ?)";
                PreparedStatement statement = conn.prepareStatement(insertRecord);
                
                statement.setString(1,fileName);
                statement.setInt(2, chunkID);
                statement.setBinaryStream(3, new ByteArrayInputStream(data),data.length);
                statement.setString(4,file_id);
                statement.setInt(5, totalNoOfChunks);

                int insertResult = statement.executeUpdate();
                if (insertResult > 0) {
                    logger.info("Insert table FileChunk in the FileDB successfully. ");
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on inserting a record: chunkId " + chunkID + " of the file " + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) { }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                   // conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean deleteRecordFileChunk(String fileName) {

        try {
            if (fileName == null || fileName.length() == 0) {
                logger.info("No record to delete.");
                return false;
            }
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String deleteRecord = "DELETE FROM FileChunk\n" +
                        "WHERE fileName='" + fileName + "';";
                logger.info(deleteRecord);
                boolean deleteResult = stmt.execute(deleteRecord);
                if (deleteResult == false) {
                    logger.info("Delete a new record from Table FileChunk in the FileDB failed. ");
                }
                else {
                    logger.info("Delete table FileChunk in the FileDB successfully. ");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return false;
        } finally {
            if (conn != null) {
                try {
                    //conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }
    

    /**
     * We don't use this method.
     * @param fileName
     * @param chunkID
     * @param data
     * @param totalNoOfChunks
     * @param file_id
     * @return
     */
    public ResultSet selectRecordFileChunk(String fileName, int chunkID, byte[] data, int totalNoOfChunks, String file_id) {

        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return null;
        }
        try {
            // TODO complete code to use JDBC
            if (conn != null){

                System.out.println("Connection successful!");
               	Statement stmt = conn.createStatement();
               	String selectRecord = "SELECT * FROM FileChunk\n" +
                       "WHERE fileName='" + fileName + "' and chunkID=" + chunkID + ";";
               	Statement statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);                        
               	
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                }
                return rs;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: chunkId " + chunkID + " of the file " + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            }

            // indicate failure
            return null;
        } finally {
            if (conn != null) {
                try {
                    //conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public boolean checkFileExist(String fileName) {

        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return false;
        }
        ArrayList<ClassFileChunkRecord> arrayList = new ArrayList<ClassFileChunkRecord>();
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String selectRecord = "SELECT count(fileName) FROM FileChunk\n" +
                        "WHERE fileName='" + fileName + "';";
                ResultSet rs = stmt.executeQuery(selectRecord);
                
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                    return false;
                }
                else {
                	rs.next();
                	if (rs.getInt(1)> 0)
                		return true;
                	else 
                		return false;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: filename "  + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        //conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // indicate failure
        }
        return false;
    }
    
    
    public boolean checkFileChunkExist(String fileName, int chunkId) {
        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return false;
        }
        ArrayList<ClassFileChunkRecord> arrayList = new ArrayList<ClassFileChunkRecord>();
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String selectRecord = "SELECT count(fileName) FROM FileChunk\n" +
                        "WHERE fileName='" + fileName + "' AND chunkId="+chunkId+";";
                ResultSet rs = stmt.executeQuery(selectRecord);
                
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                    return false;
                }
                else {
                	rs.next();
                	if (rs.getInt(1)> 0)
                		return true;
                	else 
                		return false;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: filename "  + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        //conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // indicate failure
        }
        return false;
    }
    
    
    // Only return all the chunkID of the file;
    public ArrayList<Integer> selectRecordFilenameChunkID(String fileName) {

        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return null;
        }
        ArrayList<Integer> arrayList = new ArrayList<Integer>();
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                String selectRecord = "SELECT chunkID FROM FileChunk\n" +
                        "WHERE fileName='" + fileName + "';";
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                }

                while(rs.next()){
                    int chunkIDPrint = rs.getInt(1);
                    arrayList.add(chunkIDPrint);
                }
                return arrayList;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: file " + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        //conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // indicate failure
            return null;
        }
        return null;
    }
    
    public ClassFileChunkRecord selectRecordFileChunk(String fileName, int chunkID) {

        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return null;
        }
        ArrayList<ClassFileChunkRecord> arrayList = new ArrayList<ClassFileChunkRecord>();
        
        try {
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                String selectRecord = "SELECT * FROM FileChunk\n" +
                        "WHERE fileName='" + fileName + "' and chunkID=" + chunkID + ";";
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                }

                while(rs.next()){
                    String fileNamePrint = rs.getString(1);
                    System.out.println(fileNamePrint); // should print out "1"'     fileName
                    int chunkIDPrint = rs.getInt(2);
                    System.out.println(chunkIDPrint); // should print out "2"'      chunkID
                    Blob dataPrint = rs.getBlob(3);
                    byte[] databyte = dataPrint.getBytes(1L, (int)dataPrint.length());
                    System.out.println(dataPrint); // should print out "3"'         data
                    String file_id_Print = rs.getString(4);
                    System.out.println(file_id_Print); // should print out "4"'     file_id
                    int totalNoOfChunksPrint = rs.getInt(5);
                    System.out.println(totalNoOfChunksPrint); // should print out "5"'      totalNoOfChunks
                    arrayList.add(new ClassFileChunkRecord(fileNamePrint, chunkIDPrint, databyte, totalNoOfChunksPrint, file_id_Print));
                }
                return arrayList.get(0);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: chunkId " + chunkID + " of the file " + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        //conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // indicate failure
            return null;
        }
        return null;
    }
    
    
    
    public ArrayList<ClassFileChunkRecord> selectAllRecordsFileChunk() {

        ArrayList<ClassFileChunkRecord> arrayList = new ArrayList<ClassFileChunkRecord>();
        try {
        	if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                String selectRecord = "SELECT fileName, chunkID FROM FileChunk ORDER BY fileName, chunkID ASC;";
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                    while(rs.next()){
                        String fileNamePrint = rs.getString(1);
                        int chunkIDPrint = rs.getInt(2);
                        
                        arrayList.add(new ClassFileChunkRecord(fileNamePrint, chunkIDPrint));
                    }
                }

                return arrayList;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record");
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        //conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // indicate failure
            return null;
        }
        return null;
    }
}
