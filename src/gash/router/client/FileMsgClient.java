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

import pipe.common.Common.Header;
import routing.Pipe.CommandMessage;

/**
 * front-end (proxy) to our service - functional-based
 *
 * @author gash
 *
 */
public class FileMsgClient {
    // track requests
    private long curID = 0;
    private FileUtil fileUtil;
    public FileMsgClient(String host, int port) {
        init(host, port);
        fileUtil = new FileUtil();
    }

    private void init(String host, int port) {
        FileConnection.initConnection(host, port);
    }

    public void addListener(FileListener listener) {
        FileConnection.getInstance().addListener(listener);
    }

    public void send(String fileName) {
        // construct the message to send
        fileUtil.chunkAndSend(fileName);

    }

//    public void addMessage() {
//
//        Message.UserMessage.Builder ub = Message.UserMessage.newBuilder();
//        ub.setMsgId(1);
//        ub.setMessage("hello world");
//    }

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
