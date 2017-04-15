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

import java.util.Scanner;

public class ClientApp implements CommListener {
	private MessageClient mc;

	public ClientApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		System.out.println("---> " + msg);
	}
	private void chunkAndSend(String fname){
		mc.chunkAndSend(fname);
	}

  private void deleteFile(String fname){
  }

  private void readFile(String fname){
  }

  private void menu(){
    //chunkAndSend("/home/nguyen/test.txt");
    


      Scanner scanner = new Scanner(System.in);
      String command = null;
      String[] commands;
      do{
        System.out.println("Ping\n" + 
                         "Read <fileName>\n" + 
                         "Write <filePath>\n" +
                         "Delete <fileName>\n" +
                         "quit\n");


        command = scanner.nextLine();
        commands = command.split("\\s+");
        switch(commands[0]){
          case "Ping": ping(2);
                  break;
          case "Read" : 
                  if(commands.length > 1)
                    readFile(commands[1]);
                  break;
          case "Write" : {
                  if(commands.length > 1)
                    chunkAndSend(commands[1]);
                  break;
                  }
          case "Delete" : 
                  if(commands.length >1)
                    deleteFile(commands[1]);
                  break;
          default:
                  break;
        }
      } while(!commands[0].equals("quit"));
      
  }

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "localhost";
		int port = 4168;
		try {
			MessageClient mc = new MessageClient(host, port);
			ClientApp ca = new ClientApp(mc);
      ca.menu();

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
