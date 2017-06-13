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

import java.io.IOException;
import java.time.Instant;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private Server server;

  private void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = ServerBuilder.forPort(port)
        .addService(new GreeterImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        HelloWorldServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    final HelloWorldServer server = new HelloWorldServer();
    server.start();
    server.blockUntilShutdown();
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    private Model model = new Model();

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      long age = Calendar.getInstance().get(Calendar.YEAR) - req.getYearOfBirth();
      HelloReply reply = HelloReply.newBuilder().setGreetingText(
          String.format("Hello %s, you are %s years old", req.getName(), age)).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    @Override
    public void getFeed(GetFeedRequest req, StreamObserver<GetFeedResponse> responseObserver) {
      model.getPosts(req.getUserId(), (List<Map<String, String>> records) -> {
        List<GetFeedResponse.Post> ps = records.stream()
          .filter((x) -> matches(x, req.getSearchTermsList()))
          .map(x -> toPost(x)).collect(Collectors.toList());
        
        final GetFeedResponse.Builder builder = GetFeedResponse.newBuilder();
        builder.addAllPost(ps);
        
        model.getTotalPosts(req.getUserId(), (Long n) -> {
          builder.setTotalPostCount(n);
          responseObserver.onNext(builder.build());
          responseObserver.onCompleted();
        });
      });
      
    }
    
    private GetFeedResponse.Post toPost(Map<String, String> map) {
      GetFeedResponse.Post.Builder builder = GetFeedResponse.Post.newBuilder();
      builder.setBody(map.get("body").toString());
      builder.setLastChangedInMillis(Instant.parse(map.get("last_modified")).toEpochMilli());
      builder.setTitle(map.get("title").toString());
      return builder.build();
    }

    private boolean matches(Map<String, String> map, List<String> searchTerms) {
      String body = map.get("body").toString();
      for (String curr : searchTerms) {
        if (body.contains(curr)) {
          return true;
        }
      }
      return false;
    }
    
  }
}
