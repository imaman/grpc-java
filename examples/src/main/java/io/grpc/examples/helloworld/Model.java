package io.grpc.examples.helloworld;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.google.common.base.Objects;

public class Model {
  
  private final List<Map<String, String>> data = new ArrayList<>();
  private final AmazonS3 s3client;
  
  
  public Model() {
    try {
      Properties props = new Properties();
      props.load(new FileReader("/home/imaman/demo.props"));
      AWSCredentials credentials = new BasicAWSCredentials(props.getProperty("AWS_ACCESS_KEY"), 
          props.getProperty("AWS_SECRET_KEY"));
      AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
      
      
      s3client = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.AP_NORTHEAST_1).build();
//      s3client.createBucket("cs-236700");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    data.add(newMap("1", "2017-04-28T10:37:05Z", "title_1", "word_2"));
    data.add(newMap("2", "2017-03-28T10:37:05Z", "title_2", "word_1"));
    data.add(newMap("1", "2017-05-28T10:37:05Z", "title_3", "word_3"));
  }

  private Map<String, String> newMap(String userId, String modified, String title, String body) {
    Map<String, String> result = new HashMap<>();
    result.put("user_id", userId);
    result.put("title", title);
    result.put("body", body);
    result.put("last_modified", modified);
    return result;
  }

  public void getPosts(String userId, Consumer<List<Map<String, String>>> callback) {
    TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3client).build();
    GetObjectRequest req = new GetObjectRequest("cs-236700", userId);
    
    File file;
    try {
      file = File.createTempFile("reply", ".tmp");
    } catch (IOException e) {
      callback.accept(null);
      return;
    }
    S3ProgressListener l = new S3ProgressListener() {
      
      @Override
      public void progressChanged(ProgressEvent event) {
        if (event.getEventType() == ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT) {
          try {
            byte[] buf = getData(file);
            HelloReply storedData = HelloReply.parseFrom(buf);
            callback.accept(toListMap(storedData));
          } catch (Exception e) {
            e.printStackTrace();
            callback.accept(null);
          }          
        }
        if (event.getEventType() == ProgressEventType.CLIENT_REQUEST_FAILED_EVENT) {
          System.out.println("CLIENT_REQUEST_FAILED_EVENT");
          callback.accept(null);
        }
      }

      private List<Map<String, String>> toListMap(HelloReply storedData) {
        Map<String, String> map = new HashMap<>();
        map.put("text", storedData.getGreetingText());
        List<Map<String, String>> list = new ArrayList<>();
        list.add(map);
        System.out.println("list=" + list);
        return list;
      }

      private byte[] getData(File file) throws Exception {
        FileInputStream is = new FileInputStream(file);
        List<Byte> bytes = new ArrayList<>();
        while(true) {
          int val = is.read();
          if (val < 0)
            break;
          bytes.add((byte) val);
        }
        byte[] arr = new byte[bytes.size()];
        for (int i = 0; i < arr.length; ++i)
          arr[i] = bytes.get(i);
        return arr;
      }
      
      @Override
      public void onPersistableTransfer(PersistableTransfer persistableTransfer) {}
    };
    
    tm.download(req, file, l, 4000L);     
  }
  public void getTotalPosts(String userId, Consumer<Long> callback) {
    long count = data.stream()
        .filter((Map<String, String> x) -> Objects.equal(userId, x.get("user_id"))).count();
    callback.accept(count);
  }

  public void addPost(String who, HelloReply reply, Consumer<Exception> done) {
    TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3client).build();
    byte[] bytes = reply.toByteArray();
    ObjectMetadata m = new ObjectMetadata();
    m.setContentLength(bytes.length);
    ProgressListener listener = new ProgressListener.NoOpProgressListener() {

      @Override
      public void progressChanged(ProgressEvent e) {
        ProgressEventType et = e.getEventType();
        if (et == ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT) {
          done.accept(null);
        }
        
        if (et == ProgressEventType.CLIENT_REQUEST_FAILED_EVENT) {
          done.accept(new Exception("failed to create object " + who));
        }
      }
    };
    tm.upload("cs-236700", who, new ByteArrayInputStream(bytes), m).addProgressListener(listener);
  }
}
