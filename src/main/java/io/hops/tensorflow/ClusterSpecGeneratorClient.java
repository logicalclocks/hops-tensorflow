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

import com.google.common.collect.ImmutableList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.hops.tensorflow.clusterspecgenerator.ClusterSpecGeneratorGrpc;
import io.hops.tensorflow.clusterspecgenerator.Container;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecReply;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecRequest;
import io.hops.tensorflow.clusterspecgenerator.RegisterContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

public class ClusterSpecGeneratorClient {
  
  private static final Log LOG = LogFactory.getLog(ClusterSpecGeneratorClient.class);
  
  private final ManagedChannel channel;
  private final ClusterSpecGeneratorGrpc.ClusterSpecGeneratorBlockingStub blockingStub;
  
  public ClusterSpecGeneratorClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
  }
  
  private ClusterSpecGeneratorClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = ClusterSpecGeneratorGrpc.newBlockingStub(channel);
  }
  
  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
  
  public boolean registerContainer(String applicationId, String ip, int port, String jobName, int taskIndex,
      int tbPort) {
    Container container = Container.newBuilder()
        .setApplicationId(applicationId)
        .setIp(ip)
        .setPort(port)
        .setJobName(jobName)
        .setTaskIndex(taskIndex)
        .setTbPort(tbPort)
        .build();
    RegisterContainerRequest request = RegisterContainerRequest.newBuilder().setContainer(container).build();
    try {
      blockingStub.registerContainer(request);
    } catch (StatusRuntimeException e) {
      LOG.warn("RPC failed: " + e.getStatus());
      return false;
    }
    return true;
  }
  
  public ImmutableList<Container> getClusterSpec(String applicationId) {
    GetClusterSpecRequest request = GetClusterSpecRequest.newBuilder().setApplicationId(applicationId).build();
    GetClusterSpecReply reply;
    try {
      reply = blockingStub.getClusterSpec(request);
    } catch (StatusRuntimeException e) {
      LOG.warn("RPC failed: " + e.getStatus());
      return null;
    }
    return ImmutableList.copyOf(reply.getClusterSpecList());
  }
}
