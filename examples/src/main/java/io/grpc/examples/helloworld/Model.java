package io.grpc.examples.helloworld;

import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.protobuf.InvalidProtocolBufferException;

public class Model {  
  private final AmazonDynamoDBAsync client;
  
  public Model() {
    try {
      Properties props = new Properties();
      props.load(new FileReader("/home/imaman/demo.props"));
      AWSCredentials credentials = new BasicAWSCredentials(props.getProperty("AWS_ACCESS_KEY"), 
          props.getProperty("AWS_SECRET_KEY"));
      AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
      client = AmazonDynamoDBAsyncClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.AP_NORTHEAST_1).build();     
//      restart();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  void restart() {
    try {
      deleteTable();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      createTable();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void createTable() throws Exception {
    List<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
    
    List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
    keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));
    
    CreateTableRequest ctr = new CreateTableRequest()
      .withTableName("cs_236700_posts")
      .withKeySchema(keySchema)
      .withAttributeDefinitions(attributeDefinitions)
      .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(5L)
          .withWriteCapacityUnits(6L));      
    client.createTableAsync(ctr).get();
  }

  private void deleteTable() throws Exception {
    DeleteTableRequest dtr = new DeleteTableRequest().withTableName("cs_236700_posts");
    client.deleteTableAsync(dtr).get();
  }

  public void getPosts(String userId, Consumer<Exception> err, Consumer<List<Post>> done) {
    
    
    Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
    eav.put(":val1", new AttributeValue().withS(userId));
  
    ScanRequest scanRequest = new ScanRequest().withTableName("cs_236700_posts")
        .withFilterExpression("PersonId = :val1")
        .withExpressionAttributeValues(eav);
    
    AsyncHandler<ScanRequest, ScanResult> h = new AsyncHandler<ScanRequest, ScanResult>() {
      @Override
      public void onError(Exception e) {
        System.out.println("e=" + e);
        err.accept(e);
      }

      @Override
      public void onSuccess(ScanRequest request, ScanResult result) {
        try {
          List<Post> posts = new ArrayList<>();
          for (Map<String, AttributeValue> curr : result.getItems()) {
            Post post = Post.parseFrom(curr.get("Post").getB().array());
            posts.add(post);
          }
          done.accept(posts);
        } catch (InvalidProtocolBufferException e) {
          err.accept(e);
        }
      }
    };
    client.scanAsync(scanRequest, h);
  }

  public void addEntry(AddFeedEntryRequest request, Consumer<Exception> done) {
    PutItemRequest pir = new PutItemRequest();
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("Id", new AttributeValue().withN("" + UUID.randomUUID().getLeastSignificantBits()));
    item.put("PersonId", new AttributeValue().withS(request.getUserId()));
    item.put("Post", new AttributeValue().withB(ByteBuffer.wrap(request.getPost().toByteArray())));
    pir.setItem(item);
    pir.setTableName("cs_236700_posts");
    
    AsyncHandler<PutItemRequest, PutItemResult> h = new AsyncHandler<PutItemRequest, PutItemResult>() {
      @Override
      public void onError(Exception e) {
        System.out.println("e=" + e);
        done.accept(e);
      }

      @Override
      public void onSuccess(PutItemRequest request, PutItemResult result) {
        System.out.println("pir=" + result);
        done.accept(null);
      }
    };
    System.err.println("putting " + pir);
    client.putItemAsync(pir, h);
  }

}
