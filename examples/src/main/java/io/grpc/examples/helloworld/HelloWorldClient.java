/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.grpc.examples.helloworld;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class HelloWorldClient {
  private static final Logger logger = Logger.getLogger(HelloWorldClient.class.getName());

  private final ManagedChannel channel;
  private final GreeterGrpc.GreeterStub asyncStub;

  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public HelloWorldClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext(true));
  }

  /** Construct client for accessing RouteGuide server using the existing channel. */
  HelloWorldClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    asyncStub = GreeterGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public void greet(String name, int yearOfBirth, Runnable onDone) {
    logger.info("Will try to greet " + name + " ...");
    
    StreamObserver<HelloReply> replyObserver = new StreamObserver<HelloReply>() {
      @Override public void onCompleted() { onDone.run(); }
      @Override public void onError(Throwable arg0) {}

      @Override
      public void onNext(HelloReply reply) {
        logger.info("Got reply:\n" + reply.getGreetingText());
      }
    };
    
    HelloRequest request = HelloRequest.newBuilder().setName(name).setYearOfBirth(yearOfBirth).build();
    asyncStub.sayHello(request, replyObserver);
  }
  
  public void addPost(String user, String title, String body, Runnable onDone, Consumer<Throwable> onError) {
    StreamObserver<AddFeedEntryResponse> responseObserver = new StreamObserver<AddFeedEntryResponse>() {
      @Override public void onCompleted() { onDone.run(); }
      @Override public void onError(Throwable e) { onError.accept(e); }

      @Override
      public void onNext(AddFeedEntryResponse repsonse) {
        logger.info("Got reply:\n" + repsonse.toString());
      }
    };
    
    Post post = Post.newBuilder().setTitle(title).setBody(body).build();
    AddFeedEntryRequest request = AddFeedEntryRequest .newBuilder()
        .setUserId(user)
        .setPost(post)
        .build();
    asyncStub.withDeadlineAfter(5000, TimeUnit.MILLISECONDS).addFeedEntry(request, responseObserver);    
  }
  

  public void getFeed(String userId, String searchTerm, Runnable onDone, Consumer<Throwable> onError) {
    StreamObserver<GetFeedResponse> responseObserver = new StreamObserver<GetFeedResponse>() {
      @Override public void onCompleted() { onDone.run(); }
      @Override public void onError(Throwable e) { onError.accept(e); }

      @Override
      public void onNext(GetFeedResponse repsonse) {
        System.out.println("Here is " + userId + " feed's:" + repsonse.getPostList().stream()
            .map(p -> p.getTitle() + ", " + p.getBody()).collect(Collectors.joining("; ")));
      }
    };
    
    GetFeedRequest request = GetFeedRequest.newBuilder()
        .setUserId(userId)
        .addSearchFor(searchTerm)
        .build();
    asyncStub.withDeadlineAfter(5000, TimeUnit.MILLISECONDS).getFeed(request, responseObserver);
  }
  
  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting.
   */
  public static void main(String[] args) throws Exception {
    /* Access a service running on the local machine on port 50051 */
    HelloWorldClient client = new HelloWorldClient("localhost", 50051);
    
//    CountDownLatch writeLatch = new CountDownLatch(5);
//    client.addPost("i-love-batman", "Batman Begins", "Featuring: Christian Bale", writeLatch::countDown, t -> { logger.info("f1=" + t); writeLatch.countDown(); });
//    client.addPost("i-love-batman", "The Dark Knight", "Featuring: Christian Bale", writeLatch::countDown, t -> { logger.info("f2=" + t); writeLatch.countDown(); });
//    client.addPost("FakePeterParker", "Spiderman", "Featuring: Tobey Maguire", writeLatch::countDown, t -> { logger.info("f3=" + t); writeLatch.countDown(); });
//    client.addPost("FakePeterParker", "The Amazing Spiderman", "Featuring: Andrew Garfield", writeLatch::countDown, t -> { logger.info("f4=" + t); writeLatch.countDown(); });
//    client.addPost("i-love-batman", "The Dark Knight Rises", "Featuring: Michael Caine", writeLatch::countDown, t -> { logger.info("f5=" + t); writeLatch.countDown(); });
//    writeLatch.await();

    CountDownLatch readLatch = new CountDownLatch(2);
    client.getFeed("i-love-batman", "Bale", readLatch::countDown, t -> { logger.info("f6=" + t); readLatch.countDown(); });
    client.getFeed("FakePeterParker", "", readLatch::countDown, t -> { logger.info("f7=" + t); readLatch.countDown(); });
    readLatch.await();
    
    client.shutdown();
  }
}
