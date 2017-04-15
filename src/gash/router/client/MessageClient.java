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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.common.Common.Header;
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

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
		logger.info("MessageClient init host: " + host + " port: " + port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

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
	/*
	commandMessage
		header
		request
			requestType
			payload: writeBody
				file_id
				filename
				Chunk
					chunk_id
					chunk_data
					chunk_size
	 */
	public static void chunkAndSend(String fname){
		File file = new File(fname);
		FileInputStream fis;
		FileOutputStream fos;
		int file_size = (int)file.length();
		final int CHUNK_SIZE = 1024;
		int numberOfChunks = 0;
		int readLength = CHUNK_SIZE;
		byte[] byteChunk;
		int read = 0;
		String newFileName;
		int requestId = 0;
		try{
			fis = new FileInputStream(file);
			while(file_size > 0){
				if(file_size <= CHUNK_SIZE)
					readLength = file_size;
				byteChunk = new byte[readLength];
				read = fis.read(byteChunk, 0, readLength);
				file_size -= read;
				assert (read == byteChunk.length);
				numberOfChunks++;
				//get hash key for store, to do
				logger.info("bytechunk: "+byteChunk.toString());
				newFileName = String.format("%s.part%06d",fname, numberOfChunks-1);
				CommandMessage.Builder cmdb = CommandMessage.newBuilder();
				Common.Request.Builder r = Common.Request.newBuilder();
				Common.WriteBody.Builder rb = Common.WriteBody.newBuilder();
				Common.Chunk.Builder cb = Common.Chunk.newBuilder();
				Header.Builder hb = Header.newBuilder();

				hb.setNodeId(999);
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(-1);

				r.setRequestType(Common.TaskType.WRITEFILE);

				cb.setChunkId(numberOfChunks);
				cb.setChunkData(ByteString.copyFrom(byteChunk));

				rb.setFilename(fname);
				rb.setChunk(cb);
				r.setRwb(rb);

				cmdb.setHeader(hb);
				cmdb.setRequest(r);


				logger.info("build success, start to enque");
				logger.info("msg enque is: "+cmdb.getRequest().getRwb().getChunk().toString());
				CommConnection.getInstance().enqueue(cmdb.build());
				logger.info("enque success");
//                fos = new FileOutputStream(new File(newFileName));
//                fos.write(byteChunk);
//                fos.flush();
//                fos.close();
				byteChunk = null;
				fos = null;

			}
		}catch(Exception e){
			e.printStackTrace();
		}
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
