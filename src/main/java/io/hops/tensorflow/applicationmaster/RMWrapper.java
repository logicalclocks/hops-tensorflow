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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RMWrapper {
  
  private static final Log LOG = LogFactory.getLog(RMWrapper.class);
  
  private ApplicationMaster applicationMaster;
  
  private AMRMClientAsync client;
  
  public RMWrapper(ApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
    client = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
  }
  
  public AMRMClientAsync getClient() {
    return client;
  }
  
  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    
    Map<ContainerId, Container> allAllocatedContainers = new ConcurrentHashMap<>();
    Set<ContainerId> workerIds = Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
    
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(applicationMaster.getAppAttemptID() + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());
        
        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        
        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // application failed, counts as completed
            applicationMaster.getNumCompletedContainers().incrementAndGet();
            if (workerIds.contains(containerStatus.getContainerId())) {
              applicationMaster.getNumCompletedWorkers().incrementAndGet();
            }
            applicationMaster.getNumFailedContainers().incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            applicationMaster.getNumAllocatedContainers().decrementAndGet();
            applicationMaster.getNumRequestedContainers().decrementAndGet();
            // we do not need to release the container as it would be done by the RM
          }
        } else {
          // nothing to do, container completed successfully
          applicationMaster.getNumCompletedContainers().incrementAndGet();
          if (workerIds.contains(containerStatus.getContainerId())) {
            applicationMaster.getNumCompletedWorkers().incrementAndGet();
          }
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        if (applicationMaster.getTimelineHandler().isClientNotNull()) {
          applicationMaster.getTimelineHandler().publishContainerEndEvent(containerStatus);
        }
      }
      
      // ask for more containers if any failed
      int askCount = applicationMaster.getNumTotalContainers() - applicationMaster.getNumRequestedContainers().get();
      applicationMaster.getNumRequestedContainers().addAndGet(askCount);
      
      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          AMRMClient.ContainerRequest containerAsk = applicationMaster.setupContainerAskForRM();
          client.addContainerRequest(containerAsk);
        }
      }
      
      if (applicationMaster.getNumCompletedWorkers().get() == applicationMaster.getNumWorkers()) {
        applicationMaster.setDone();
      }
    }
    
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
      applicationMaster.getNumAllocatedContainers().addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        allAllocatedContainers.put(allocatedContainer.getId(), allocatedContainer);
      }
      if (applicationMaster.getNumAllocatedContainers().get() == applicationMaster.getNumTotalContainers()) {
        assert applicationMaster.getNumAllocatedContainers().get() == allAllocatedContainers.size();
        launchAllContainers();
      }
    }
    
    private void launchAllContainers() {
      int worker = -1;
      int ps = -1;
      for (Container allocatedContainer : allAllocatedContainers.values()) {
        LOG.info("Launching yarntf application on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerResourceMemory"
            + allocatedContainer.getResource().getMemory()
            + ", containerResourceVirtualCores"
            + allocatedContainer.getResource().getVirtualCores());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());
        
        String jobName;
        int taskIndex;
        
        if (worker < applicationMaster.getNumWorkers() - 1) {
          jobName = "worker";
          taskIndex = ++worker;
          workerIds.add(allocatedContainer.getId());
        } else if (ps < applicationMaster.getNumPses() - 1) {
          jobName = "ps";
          taskIndex = ++ps;
        } else {
          throw new IllegalStateException("Too many TF tasks: worker " + worker + ", ps: " + ps);
        }
        
        Thread launchThread = new Thread(applicationMaster
            .createLaunchContainerRunnable(allocatedContainer, jobName, taskIndex));
        
        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        applicationMaster.addLaunchThread(launchThread);
        launchThread.start();
      }
    }
    
    @Override
    public void onShutdownRequest() {
      applicationMaster.setDone();
    }
    
    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }
    
    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) applicationMaster.getNumCompletedWorkers().get()
          / applicationMaster.getNumWorkers();
      return progress;
    }
    
    @Override
    public void onError(Throwable e) {
      applicationMaster.setDone();
      client.stop();
    }
  }
}
