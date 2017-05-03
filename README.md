# cmpe275Project1
## Team project on CMPE275 

1. Download MySQL 5.7.18 
2. Create a database called “FileDB” with a table named “FileChunk” in MySQL. 
```
create database FileDB
TestSQLOperations testSQLOperations = new TestSQLOperations();
testSQLOperations.createTable();
```
3. Download Google Protocol Buffer v3.2. 
4. Build the protobuf files: 
	```
	./build_pb.sh. 
	```
5. Compile all java files using Apache Ant: 
	```
	ant build 
	```
6. Start the server : 
	```
	./startServer.sh <config file> 
	```
	Note: the config file must have the ips and ports of all the nodes in the network. 
7. Start the client: 
	```
	./startClient.sh <cluster_id>
	```
	The client has 6 operations:   
	
        1. ping <cluster id>: ping to a cluster.
        
        2. ls: retrieve a list of all files stored in the server
        
        3. leader: get leader’s ip and port from redis server
        
        4. read <fileName> : retrieve a file from the network.
        
        5. write <fileName>:  upload a file from the client’s project directory to the network.
        
        6. quit: exit the client.

