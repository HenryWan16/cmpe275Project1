package gash.router.client;

import com.google.protobuf.ByteString;
import file.FileOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;

/**
 * Created by sam on 4/13/17.
 * chunk-thread-send individually
 */
public class FileUtil {

    protected static Logger logger = LoggerFactory.getLogger("server");

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

                newFileName = String.format("%s.part%06d",fname, numberOfChunks-1);

                FileOuterClass.Request.Builder r = FileOuterClass.Request.newBuilder();
                r.setRequestType(FileOuterClass.RequestType.WRITE);
                FileOuterClass.File.Builder fb = FileOuterClass.File.newBuilder();
                fb.setChunkId(numberOfChunks);
                fb.setData(ByteString.copyFrom(byteChunk));
                fb.setFilename(fname);
                r.setFile(fb);
                r.setRequestId(newFileName);
                FileOuterClass.Request req = r.build();
                logger.info("build success, start to enque");
                logger.info("msg enque is: "+req.toString());
                FileConnection.getInstance().enqueue(req);
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

//    public void sendFile(String fileName){
//        file.FileOuterClass.Request.Builder r = file.FileOuterClass.Request.newBuilder();
//        r.setRequestType(FileOuterClass.RequestType.WRITE);
//        FileOuterClass.File.Builder fb = FileOuterClass.File.newBuilder();
//        fb.set
//        r.setFile()
//    }


}
