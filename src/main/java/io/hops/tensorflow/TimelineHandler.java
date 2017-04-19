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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

public class TimelineHandler {
  
  private static final Log LOG = LogFactory.getLog(TimelineHandler.class);
  
  private String appAttemptId;
  private String domainId;
  private UserGroupInformation ugi;
  private TimelineClient timelineClient;
  
  public TimelineHandler(String appAttemptId, String domainId, UserGroupInformation ugi) {
    this.appAttemptId = appAttemptId;
    this.domainId = domainId;
    this.ugi = ugi;
  }
  
  public void startClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            // Creating the Timeline Client
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
          } else {
            timelineClient = null;
            LOG.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }
  
  public void stopClient() {
    timelineClient.stop();
  }
  
  public boolean isClientNotNull() {
    return timelineClient != null;
  }
  
  public void publishContainerStartEvent(Container container) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(ApplicationMaster.YarntfEntity.YARNTF_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(ApplicationMaster.YarntfEvent.YARNTF_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);
    
    try {
      ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          return timelineClient.putEntities(entity);
        }
      });
    } catch (Exception e) {
      LOG.error("Container start event could not be published for "
              + container.getId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }
  
  public void publishContainerEndEvent(ContainerStatus container) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(ApplicationMaster.YarntfEntity.YARNTF_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(ApplicationMaster.YarntfEvent.YARNTF_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(), e);
    }
  }
  
  public void publishApplicationAttemptEvent(ApplicationMaster.YarntfEvent appEvent) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(ApplicationMaster.YarntfEntity.YARNTF_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter("user", ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      timelineClient.putEntities(entity);
    } catch (YarnException | IOException e) {
      LOG.error("App Attempt "
          + (appEvent.equals(ApplicationMaster.YarntfEvent.YARNTF_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptId.toString(), e);
    }
  }
}
