package gash.router.app;

import file.FileOuterClass;
import gash.router.client.FileListener;
import gash.router.client.FileMsgClient;

/**
 * Created by sam on 4/13/17.
 */
public class DemoFile implements FileListener {


    @Override
    public String getListenerID() {
        return null;
    }

    @Override
    public void onMessage(FileOuterClass.Request msg) {

    }

    public static void main(String[] args){
        String host = "localhost";
        int port = 4668;
        String fileName = "/Users/sam/Documents/2017spring/cmpe275/project1/cmpe275Project1/test.txt";
        FileMsgClient fileMsgClient = new FileMsgClient(host, port);
        fileMsgClient.send(fileName);
    }
}
