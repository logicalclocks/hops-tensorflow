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

import io.hops.tensorflow.ApplicationMaster.YarntfTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
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
        applicationMaster.getNumCompletedContainers().incrementAndGet();
        if (workerIds.contains(containerStatus.getContainerId())) {
          applicationMaster.getNumCompletedWorkers().incrementAndGet();
        }
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          applicationMaster.getNumFailedContainers().incrementAndGet();
          LOG.info("Container failed." + ", containerId=" + containerStatus.getContainerId());
        } else {
          // nothing to do, container completed successfully
          LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
        }
        
        if (applicationMaster.getTimelineHandler().isClientNotNull()) {
          applicationMaster.getTimelineHandler().publishContainerEndEvent(containerStatus);
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
        LOG.info("Allocated container, resource=" + allocatedContainer.getResource());
        allAllocatedContainers.put(allocatedContainer.getId(), allocatedContainer);
      }
      if (applicationMaster.getNumAllocatedContainers().get() == applicationMaster.getNumTotalContainers()) {
        assert applicationMaster.getNumAllocatedContainers().get() == allAllocatedContainers.size();
        launchAllContainers();
      }
    }
    
    private void launchAllContainers() {
      int workerId = -1;
      int psId = -1;
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
  
        YarntfTask task;
        
        // decide task
        if (applicationMaster.getContainerGPUs() > 0) {
          // if GPU: by resource
          if (allocatedContainer.getResource().getGPUs() > 0) {
            task = YarntfTask.YARNTF_WORKER;
          } else {
            task = YarntfTask.YARNTF_PS;
          }
        } else {
          // if not: by counter
          if (workerId < applicationMaster.getNumWorkers() - 1) {
            task = YarntfTask.YARNTF_WORKER;
          }  else {
            task = YarntfTask.YARNTF_PS;
          }
        }
        
        // set task
        if (task == YarntfTask.YARNTF_WORKER) {
          jobName = "worker";
          taskIndex = ++workerId;
          workerIds.add(allocatedContainer.getId());
        } else {
          jobName = "ps";
          taskIndex = ++psId;
        }
        
        if (workerId >= applicationMaster.getNumWorkers() || psId >= applicationMaster.getNumWorkers()) {
          throw new IllegalStateException("Too many TF tasks: workers " + workerId + ", pses: " + psId);
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
      float progress = (float) applicationMaster.getNumCompletedWorkers().get() / applicationMaster.getNumWorkers();
      return progress;
    }
    
    @Override
    public void onError(Throwable e) {
      applicationMaster.setDone();
      client.stop();
    }
  }
}
