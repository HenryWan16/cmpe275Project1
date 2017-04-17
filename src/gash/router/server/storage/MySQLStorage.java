package gash.router.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.messages.QOSWorker;

import java.sql.*;
import java.util.ArrayList;


/**
 * Created by henrywan16 on 4/16/17.
 */
public class MySQLStorage {
    protected static Logger logger = LoggerFactory.getLogger("MySQL");

    private Connection conn;
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
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/FileDB?&useSSL=true", "root", "cmpe275");
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

    /**
     * testing database can be used.
     * @return
     */
    public boolean testSQL() {
        boolean result = false;
        try {
            // Do something with the Connection
            Statement stmt = conn.createStatement();
            String createStmt = "CREATE TABLE Persons\n" +
                    "(\n" +
                    "PersonID int,\n" +
                    "LastName varchar(255),\n" +
                    "FirstName varchar(255),\n" +
                    "Address varchar(255),\n" +
                    "City varchar(255),\n" +
                    "Primary Key(PersonID, Address)\n" +
                    ");\n";
            result = stmt.execute(createStmt);
            result = stmt.execute("INSERT into Persons (PersonID, LastName, FirstName, Address, City)\n" +
                    "VALUES (1, 'Henry', 'Wan','Modern', 'SJ');");
            result = stmt.execute("INSERT into Persons (PersonID, LastName, FirstName, Address, City)\n" +
                    "VALUES (2, 'Jerry', 'Gao','SJSU', 'CA');");
            result = stmt.execute("INSERT into Persons (PersonID, LastName, FirstName, Address, City)\n" +
                    "VALUES (3, 'John', 'White','ICE', 'SJ');");
            result = stmt.execute("INSERT into Persons (PersonID, LastName, FirstName, Address, City)\n" +
                    "VALUES (4, 'Tom', 'Nash','Done', 'SJ');");
            ResultSet rs=stmt.executeQuery("select * from Persons");
            while(rs.next())
                System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3));
            conn.close();
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return result;
    }

    public boolean createTable() {
        init();
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                logger.info("FileDB Connection successful!");
                Statement stmt = conn.createStatement();
                String createTable = "CREATE TABLE FileChunk\n" +
                        "(\n" +
                        "fileName varchar(255),\n" +
                        "chunkID int,\n" +
                        "data varchar(10240),\n" +
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
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean dropTable() {
        init();
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
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean insertRecordFileChunk(String fileName, int chunkID, byte[] data, int totalNoOfChunks, String file_id) {
        init();
        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to insert.");
            return false;
        }
        try {
            String str = "";
            // TODO complete code to use JDBC
            if (conn != null) {
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                if (data != null) {
                    str = new String(data);
                }
                String insertRecord = "INSERT INTO FileChunk (fileName, chunkID, data, file_id, totalNoOfChunks)\n" +
                        "VALUES ('" + fileName + "'," + chunkID + ",'" + str + "','" + file_id + "'," + totalNoOfChunks + ");";
                logger.info(insertRecord);
                boolean insertResult = stmt.execute(insertRecord);
                if (insertResult == true) {
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
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean deleteRecordFileChunk(String fileName, int chunkID) {
        init();
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
                        "WHERE fileName='" + fileName + "' and chunkID=" + chunkID + ";";
                logger.info(deleteRecord);
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
            // release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on deleting a record: chunkId " + chunkID + " of the file " + fileName, ex);
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

    public int updateRecordFileChunk(String fileName, int chunkID, byte[] data, int totalNoOfChunks, String file_id) {
        init();
        int result = 0;
        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to update.");
            return -1;
        }
        try {
            String str = "";
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
                if (data != null) {
                    str = new String(data);
                }
                String updateRecord = "UPDATE FileChunk\n" +
                        "SET fileName='" + fileName + "', chunkID=" + chunkID + ", data='" + str + "', totalNoOfChunks=" + totalNoOfChunks + ", file_id='" + file_id + "'\n" +
                        "WHERE fileName='" + fileName + "' and chunkID=" + chunkID + ";";
                logger.info(updateRecord);
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
            // release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on updating a record: chunkId " + chunkID + " of the file " + fileName, ex);
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
        init();
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
                logger.info(selectRecord);
                ResultSet rs = stmt.executeQuery(selectRecord);
                if (rs == null) {
                    logger.info("No record in Table FileChunk in the FileDB. Selecting...");
                }
                else {
                    logger.info("Select table FileChunk in the FileDB successfully. ");
                }
                return rs;
//                ResultSet rs = stmt.executeQuery("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"); // do something with the connection.
//                while(rs.next()){
//                    System.out.println(rs.getString(1)); // should print out "1"'
//                }
            }
            // release(); // shutdown connection pool.
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
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public ArrayList<ClassFileChunkRecord> selectRecordFileChunk(String fileName, int chunkID) {
        init();
        if (fileName == null || fileName.length() == 0) {
            logger.info("No record to select.");
            return null;
        }
        ArrayList<ClassFileChunkRecord> arrayList = new ArrayList<ClassFileChunkRecord>();
        try {
            // TODO complete code to use JDBC
            if (conn != null){
                System.out.println("Connection successful!");
                Statement stmt = conn.createStatement();
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
                    String dataPrint = rs.getString(3);
                    byte[] databyte = dataPrint.getBytes();
                    System.out.println(dataPrint); // should print out "3"'         data
                    String file_id_Print = rs.getString(4);
                    System.out.println(file_id_Print); // should print out "4"'     file_id
                    int totalNoOfChunksPrint = rs.getInt(5);
                    System.out.println(totalNoOfChunksPrint); // should print out "5"'      totalNoOfChunks
                    arrayList.add(new ClassFileChunkRecord(fileNamePrint, chunkIDPrint, databyte, totalNoOfChunksPrint, file_id_Print));
                }
                return arrayList;
            }
            // release(); // shutdown connection pool.
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("failed/exception on selecting a record: chunkId " + chunkID + " of the file " + fileName, ex);
            try {
                conn.rollback();
            } catch (SQLException e) {
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
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
