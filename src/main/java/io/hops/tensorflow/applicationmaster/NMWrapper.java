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
package io.hops.tensorflow.applicationmaster;

import io.hops.tensorflow.ApplicationMaster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NMWrapper {
  
  private static final Log LOG = LogFactory.getLog(NMWrapper.class);
  
  private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();
  private Set<ContainerId> workerIds = Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
  private final ApplicationMaster applicationMaster;
  
  private NMClientAsync client;
  
  public NMWrapper(ApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
    client = new NMClientAsyncImpl(new CallbackHandler());
  }
  
  public NMClientAsync getClient() {
    return client;
  }
  
  public synchronized void addContainer(ContainerId containerId, Container container, boolean worker) {
    containers.putIfAbsent(containerId, container);
    if (worker) {
      workerIds.add(containerId);
    }
  }
  
  private class CallbackHandler implements NMClientAsync.CallbackHandler {
    
    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }
    
    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }
    
    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        client.getContainerStatusAsync(containerId, container.getNodeId());
      }
      if (applicationMaster.getTimelineHandler().isClientNotNull()) {
        applicationMaster.getTimelineHandler().publishContainerStartEvent(container);
      }
    }
    
    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
      containers.remove(containerId);
      applicationMaster.getNumCompletedContainers().incrementAndGet();
      if (workerIds.contains(containerId)) {
        applicationMaster.getNumCompletedWorkers().incrementAndGet();
      }
      applicationMaster.getNumFailedContainers().incrementAndGet();
    }
    
    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }
    
    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }
  }
}
