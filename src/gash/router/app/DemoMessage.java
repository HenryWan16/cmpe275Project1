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
package gash.router.app;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import routing.Pipe.CommandMessage;

public class DemoMessage implements CommListener{
    private MessageClient mc;

    public DemoMessage(MessageClient mc) {
        init(mc);
    }

    private void init(MessageClient mc) {
        this.mc = mc;
        this.mc.addListener(this);
        System.out.println("addListener(DemoMessage object)");
    }

    private void sendMessage(String message, int fromNode, int toNode) {
        long previousTime = System.currentTimeMillis(), futureTime = 0;
        System.out.println("=====================sendMessage======================");
        mc.sendMessage(message, fromNode, toNode);
        futureTime = System.currentTimeMillis();
        System.out.println("Message sending times (msec)");
        System.out.println(futureTime - previousTime);
    }

    @Override
    public String getListenerID() {
        return "demoMessage";
    }

    @Override
    public void onMessage(CommandMessage msg) {
        System.out.println("---> " + msg);
        System.out.println("DemoMessage onMessage ");
    }

    /**
     * sample application (client) use of our messaging service;
     * send a message to localhost:4667;
     *
     * @param args
     */
    public static void main(String[] args) {
        int nodeId = 1, destinationId = 6;
        String message = "How are you?";

        // Send message by current node. Current node is 1
        String currentHost = "localhost";
        int currentPort = 4167;
        try {
            MessageClient mc = new MessageClient(currentHost, currentPort);
            DemoMessage dm = new DemoMessage(mc);

            // do stuff w/ the connection
            dm.sendMessage(message, nodeId, destinationId);

            System.out.println("\n** exiting in 10 seconds. **");
            System.out.flush();
            Thread.sleep(10 * 1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CommConnection.getInstance().release();
        }
    }
}
