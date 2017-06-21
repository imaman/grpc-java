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

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.helloworld.Model.Cs236700SetupData;
import io.grpc.stub.StreamObserver;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class HelloWorldServer {
  private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

  private final MetricExporter exporter;
  private final Model model;
  private Server server;

  @Inject
  public HelloWorldServer(Model model, MetricExporter exporter) {
    this.model = model;
    this.exporter = exporter;
  }

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
  
  
  private static class Module extends AbstractModule {
    @Override
    protected void configure() {
      bind(MetricExporter.class).in(Singleton.class);
      bind(HelloWorldServer.class).in(Singleton.class);
      bind(Model.class).in(Singleton.class);
      
      try {
        Properties props = new Properties();
        props.load(new FileReader("/home/imaman/demo.props"));
        bind(Properties.class).annotatedWith(Cs236700SetupData.class).toInstance(props);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    Injector injector = Guice.createInjector(new Module());
    HelloWorldServer server = injector.getInstance(HelloWorldServer.class);
    server.start();
    server.blockUntilShutdown();
  }

  private class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    
    @Override
    public void getFeed(GetFeedRequest req, StreamObserver<GetFeedResponse> responseObserver) {
      exporter.increment("incoming_rpc.getFeed", 1);
      final GetFeedResponse.Builder builder = GetFeedResponse.newBuilder();
      
      Consumer<Exception> onErr = (Exception e) -> {
        exporter.increment("responding_error", 1);
        responseObserver.onError(e); 
      };
      model.getPosts(req.getUserId(), onErr,
          (List<Post> posts) -> {
            for (Post curr : posts) {
              if (matches(req, curr)) 
                builder.addPost(curr);
            }
            exporter.increment("model.num_posts", posts.size());
            exporter.increment("model.num_matched_posts", builder.getPostCount());
            exporter.increment("responding_error", 0);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
          });
    }

    private boolean matches(GetFeedRequest req, Post post) {
      String body = post.getBody();
      for (String term : req.getSearchForList()) {
        if (body.contains(term)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void addFeedEntry(AddFeedEntryRequest request, StreamObserver<AddFeedEntryResponse> responseObserver) {
      exporter.increment("incoming_rpc.addFeedEntry", 1);
      Consumer<Exception> c = (Exception e) -> {
        if (e == null) 
          responseObserver.onCompleted();
        else 
          responseObserver.onError(e);
      };
      model.addEntry(request, c);
    }
  }
}
