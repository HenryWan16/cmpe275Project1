/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.client;

import com.google.protobuf.ByteString;

import gash.router.server.raft.MessageUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.common.Common.Header;
import pipe.common.Common.TaskType;
import routing.Pipe.CommandMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	protected static Logger logger = LoggerFactory.getLogger("client");
	// track requests
	private long curID = 0;
	private MergeWorker mw = null;

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
		logger.info("MessageClient init host: " + host + " port: " + port);
		this.mw = new MergeWorker();
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(-1);
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			logger.info("MessageClient send CommandMessage with ping=true to Netty Channel! ");
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void chunkAndSend(String fname){
		File file = new File(fname);
		FileInputStream fis;

		int file_size = (int)file.length();
		final int CHUNK_SIZE = 2 * 1024;
		int numberOfChunks = 0;
		int readLength = CHUNK_SIZE;
		byte[] byteChunk;
		int read = 0;

		int chunkSize = file_size / readLength + (file_size % readLength == 0 ? 0 : 1);
		
		try {
			fis = new FileInputStream(file);
			logger.info("file_size = " + file_size);
			while(file_size > 0) {
				if(file_size <= CHUNK_SIZE)
					readLength = file_size;
				byteChunk = new byte[readLength];
				read = fis.read(byteChunk, 0, readLength);
				file_size -= read;
				assert (read == byteChunk.length);
				numberOfChunks++;
				// get hash key for store, to do
				logger.info("bytechunk: "+byteChunk.toString());

				CommandMessage cm = MessageUtil.buildCommandMessage(MessageUtil.buildHeader(999,System.currentTimeMillis()),null,
						MessageUtil.buildRequest(TaskType.WRITEFILE, MessageUtil.buildWriteBody(-1,fname,"txt",
								MessageUtil.buildChunk(numberOfChunks,byteChunk,chunkSize),
								chunkSize),null),null);

				logger.info("build success, start to enque");
				logger.info("msg enque is: "+cm.getRequest().getRwb().getChunk().toString());
				CommConnection.getInstance().enqueue(cm);
				logger.info("enque success");
				byteChunk = null;
			}
			fis.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	///
	
	//send file request to server
	public void sendReadRequest(String fname){
		/*
		* commandmessage
		* 	header
		* 	request
			* requesttype
				* readfile
			readbody

		 */
//		if (fname.equals("log.txt")) {
//			CommandMessage cmdb = MessageUtil.buildCommandMessage(
//				MessageUtil.buildHeader(999, System.currentTimeMillis()),
//				null,
//				MessageUtil.buildRequest(TaskType.READFILE, null,
//						MessageUtil.buildReadBody(fname, -1, -1, -1)),
//				null);
//			try {
//				CommConnection.getInstance().enqueue(cmdb);
//				
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//		}else{
//			CommandMessage cmdb = MessageUtil.buildCommandMessage(MessageUtil.buildHeader(999,System.currentTimeMillis()),null,
//					MessageUtil.buildRequest(TaskType.READFILE,null,
//							MessageUtil.buildReadBody(fname,-1,-1,-1)),null);
//
//			try {
//				CommConnection.getInstance().enqueue(cmdb);
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
		
		// send a request to the server to read the file.
		CommandMessage cmdb = MessageUtil.buildCommandMessage(MessageUtil.buildHeader(999,System.currentTimeMillis()),null,
				MessageUtil.buildRequest(TaskType.READFILE,null,
						MessageUtil.buildReadBody(fname,-1,-1,-1)),null);

		try {
			logger.info("The first time to send Read message to server " + cmdb);
			CommConnection.getInstance().enqueue(cmdb);

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//start the thread for waiting the chunks from server
		this.mw.setResultFileName("files/result.txt");
		this.mw.successMerge = false;
		Thread cthread = new Thread(this.mw);
		cthread.start();
	}

	/**
	 * send a comm message through workPort to the Remote.
	 * @param message
	 * @author Henry
	 */
	public void sendCommMessage(String message, int fromNodeId, int toNodeId) {
		// construct the message to send
		Common.Header.Builder hb = Common.Header.newBuilder();
		hb.setNodeId(fromNodeId);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(toNodeId);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(false);
//		rb.setMessage(message);
//		logger.info("rb.hasMessage() = " + rb.hasMessage());
		try {
			// using queue
//            logger.info("MessageClient send CommandMessage with message=True to Netty Channel! ");
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendDeleteFile(String fname) {
		CommandMessage cmdb = MessageUtil.buildCommandMessage(
				MessageUtil.buildHeader(999, System.currentTimeMillis()),
				null,
				MessageUtil.buildRequest(TaskType.DELETEFILE, null,
						MessageUtil.buildReadBody(fname, -1, -1, -1)),
				null);
		try {
			CommConnection.getInstance().enqueue(cmdb);
		} 
		catch (Exception e) { e.printStackTrace(); }
	}
	
	public void release() {
		CommConnection.getInstance().release();
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized long nextId() {
		return ++curID;
	}
}
