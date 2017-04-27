/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.tensorflow;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.hops.tensorflow.clusterspecgenerator.ClusterSpecGeneratorGrpc;
import io.hops.tensorflow.clusterspecgenerator.Container;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecReply;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecRequest;
import io.hops.tensorflow.clusterspecgenerator.RegisterContainerReply;
import io.hops.tensorflow.clusterspecgenerator.RegisterContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSpecGeneratorServer {
  
  private static final Log LOG = LogFactory.getLog(ClusterSpecGeneratorServer.class);
  
  private String applicationId;
  private int numContainers;
  private int numWorkers;
  private Server server;
  private ClusterSpecGeneratorImpl clusterSpecGeneratorImpl;
  
  public ClusterSpecGeneratorServer(String applicationId, int numContainers, int numWorkers) {
    this.applicationId = applicationId;
    this.numContainers = numContainers;
    this.numWorkers = numWorkers;
  }
  
  public void start(int port) throws IOException {
    if (server != null) {
      throw new IllegalStateException("Already started");
    }
    clusterSpecGeneratorImpl = new ClusterSpecGeneratorImpl();
    server = ServerBuilder.forPort(port)
        .addService(clusterSpecGeneratorImpl)
        .build()
        .start();
    LOG.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        ClusterSpecGeneratorServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }
  
  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }
  
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
  
  public Map<String, Container> getClusterSpec() {
    return clusterSpecGeneratorImpl.clusterSpec;
  }
  
  public Map<String, String> getTensorBoards() {
    return clusterSpecGeneratorImpl.tensorBoards;
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    ClusterSpecGeneratorServer server = new ClusterSpecGeneratorServer("(appId)", 3, 3);
    server.start(50052);
    server.blockUntilShutdown();
  }
  
  private class ClusterSpecGeneratorImpl extends ClusterSpecGeneratorGrpc.ClusterSpecGeneratorImplBase {
    
    Map<String, Container> clusterSpec = new ConcurrentHashMap<>();
    Map<String, String> tensorBoards = new ConcurrentHashMap<>();
    
    @Override
    public void registerContainer(RegisterContainerRequest request,
        StreamObserver<RegisterContainerReply> responseObserver) {
      Container container = request.getContainer();
      LOG.info("Received registerContainerRequest: (" + container.getApplicationId() + ", " + container.getIp()
          + ", " + container.getPort() + ", " + container.getJobName() + ", " + container.getTaskIndex() + ", " +
          container.getTbPort() + ")");
      if (container.getApplicationId().equals(applicationId)) {
        clusterSpec.put(container.getJobName() + container.getTaskIndex(), container);
        if (container.getTbPort() > 0) {
          tensorBoards.put(container.getJobName() + container.getTaskIndex(),
              container.getIp() + ":" + container.getTbPort());
        }
        if (clusterSpec.size() > numContainers) {
          throw new IllegalStateException("clusterSpec size: " + clusterSpec.size());
        }
        RegisterContainerReply reply = RegisterContainerReply.newBuilder().build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
    
    @Override
    public void getClusterSpec(GetClusterSpecRequest request, StreamObserver<GetClusterSpecReply> responseObserver) {
      LOG.info("Received getClusterSpecRequest: " + request.getApplicationId());
      if (request.getApplicationId().equals(applicationId)) {
        GetClusterSpecReply reply;
        if (clusterSpec.size() == numContainers) {
          List<Container> clusterSpecList = new ArrayList<>(clusterSpec.values());
          Collections.sort(clusterSpecList, new Comparator<Container>() {
            @Override
            public int compare(Container c1, Container c2) {
              return (c1.getTaskIndex() < c2.getTaskIndex() ? -1 : (c1.getTaskIndex() == c2.getTaskIndex() ? 0 : 1));
            }
          });
          reply = GetClusterSpecReply.newBuilder().addAllClusterSpec(clusterSpecList).build();
        } else {
          reply = GetClusterSpecReply.newBuilder().build();
        }
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
  }
}
