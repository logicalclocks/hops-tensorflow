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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static io.hops.tensorflow.ClientArguments.AM_JAR;
import static io.hops.tensorflow.ClientArguments.AM_MEMORY;
import static io.hops.tensorflow.ClientArguments.AM_PRIORITY;
import static io.hops.tensorflow.ClientArguments.AM_VCORES;
import static io.hops.tensorflow.ClientArguments.ARGS;
import static io.hops.tensorflow.ClientArguments.ATTEMPT_FAILURES_VALIDITY_INTERVAL;
import static io.hops.tensorflow.ClientArguments.CREATE;
import static io.hops.tensorflow.ClientArguments.DEBUG;
import static io.hops.tensorflow.ClientArguments.DOMAIN;
import static io.hops.tensorflow.ClientArguments.ENV;
import static io.hops.tensorflow.ClientArguments.FILES;
import static io.hops.tensorflow.ClientArguments.HELP;
import static io.hops.tensorflow.ClientArguments.KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS;
import static io.hops.tensorflow.ClientArguments.LOG_PROPERTIES;
import static io.hops.tensorflow.ClientArguments.MAIN;
import static io.hops.tensorflow.ClientArguments.MAX_APP_ATTEMPTS;
import static io.hops.tensorflow.ClientArguments.MEMORY;
import static io.hops.tensorflow.ClientArguments.MODIFY_ACLS;
import static io.hops.tensorflow.ClientArguments.NAME;
import static io.hops.tensorflow.ClientArguments.NODE_LABEL_EXPRESSION;
// import static io.hops.tensorflow.ClientArguments.PRIORITY;
import static io.hops.tensorflow.ClientArguments.PSES;
import static io.hops.tensorflow.ClientArguments.QUEUE;
import static io.hops.tensorflow.ClientArguments.APPLICATION_TIMEOUT;
import static io.hops.tensorflow.ClientArguments.VCORES;
import static io.hops.tensorflow.ClientArguments.VIEW_ACLS;
import static io.hops.tensorflow.ClientArguments.WORKERS;
import static io.hops.tensorflow.ClientArguments.createOptions;
import static io.hops.tensorflow.CommonArguments.ALLOCATION_TIMEOUT;
import static io.hops.tensorflow.CommonArguments.GPUS;
import static io.hops.tensorflow.CommonArguments.TENSORBOARD;

public class Client {
  
  private static final Log LOG = LogFactory.getLog(Client.class);
  
  // Configuration
  private Configuration conf;
  private YarnClient yarnClient;
  
  // App master config
  private int amPriority;
  private String amQueue;
  private int amMemory;
  private int amVCores;
  private String amJar; // path
  private final String appMasterMainClass; // class name
  
  // TF application config
  // private int priority;
  private long allocationTimeout;
  private String name;
  private String mainPath;
  private String mainRelativePath; // relative for worker or ps
  private String[] arguments; // to be passed to the application
  private int numWorkers;
  private int numPses;
  private int memory;
  private int vcores;
  private int gpus;
  private Map<String, String> environment = new HashMap<>(); // environment variables
  private boolean tensorboard;
  
  private String nodeLabelExpression;
  private String log4jPropFile; // if available, add to local resources and set into classpath
  
  // Timeout threshold for client. Kill app after time interval expires
  private final long clientStartTime = System.currentTimeMillis();
  private long clientTimeout;
  
  private int maxAppAttempts;
  private boolean keepContainers; // keep containers across application attempts.
  private long attemptFailuresValidityInterval;
  
  private boolean debugFlag;
  
  // Timeline config
  private String domainId;
  private boolean toCreateDomain; // whether to create the domain of the given ID
  private String viewACLs; // reader access control
  private String modifyACLs; // writer access control
  
  // Command line options
  private Options opts;
  private CommandLine cliParser;
  
  /**
   * @param args
   *     Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      LOG.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        System.exit(-1);
      }
      result = client.run();
    } catch (Throwable t) {
      LOG.fatal("Error running Client", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.error("Application failed to complete successfully");
    System.exit(2);
  }
  
  /**
   */
  public Client(Configuration conf) throws Exception {
    this(ApplicationMaster.class.getName(), conf);
  }
  
  Client(String appMasterMainClass, Configuration conf) {
    this.conf = conf;
    this.appMasterMainClass = appMasterMainClass;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    opts = createOptions();
  }
  
  /**
   */
  public Client() throws Exception {
    this(new YarnConfiguration());
  }
  
  /**
   * Helper function to print out usage
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }
  
  /**
   * Parse command line options
   *
   * @param args
   *     Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException {
    
    cliParser = new GnuParser().parse(opts, args);
    
    if (args.length == 0) {
      printUsage();
      throw new IllegalArgumentException("No args specified for client to initialize");
    }
    
    if (cliParser.hasOption(LOG_PROPERTIES)) {
      String log4jPath = cliParser.getOptionValue(LOG_PROPERTIES);
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }
    
    if (cliParser.hasOption(HELP)) {
      printUsage();
      return false;
    }
    
    if (cliParser.hasOption(DEBUG)) {
      debugFlag = true;
    }
    
    maxAppAttempts = Integer.parseInt(cliParser.getOptionValue(MAX_APP_ATTEMPTS, "1"));
    
    if (cliParser.hasOption(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)) {
      LOG.info(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS);
      keepContainers = true;
    }
    
    name = cliParser.getOptionValue(NAME);
    amPriority = Integer.parseInt(cliParser.getOptionValue(AM_PRIORITY, "0"));
    amQueue = cliParser.getOptionValue(QUEUE, "default");
    amMemory = Integer.parseInt(cliParser.getOptionValue(AM_MEMORY, "192"));
    amVCores = Integer.parseInt(cliParser.getOptionValue(AM_VCORES, "1"));
    
    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
          + " Specified virtual cores=" + amVCores);
    }
    
    if (!cliParser.hasOption(AM_JAR)) {
      throw new IllegalArgumentException("No jar file specified for application master");
    }
    amJar = cliParser.getOptionValue(AM_JAR);
    
    if (!cliParser.hasOption(MAIN)) {
      throw new IllegalArgumentException("No main application file specified");
    }
    mainPath = cliParser.getOptionValue(MAIN);
    
    if (cliParser.hasOption(ARGS)) {
      arguments = cliParser.getOptionValues(ARGS);
    }
    if (cliParser.hasOption(ENV)) {
      String envs[] = cliParser.getOptionValues(ENV);
      for (String env : envs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          environment.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        environment.put(key, val);
      }
    }
    // priority = Integer.parseInt(cliParser.getOptionValue(PRIORITY, "0"));
    allocationTimeout = Long.parseLong(cliParser.getOptionValue(ALLOCATION_TIMEOUT, "15")) * 1000;
    
    memory = Integer.parseInt(cliParser.getOptionValue(MEMORY, "1024"));
    vcores = Integer.parseInt(cliParser.getOptionValue(VCORES, "1"));
    gpus = Integer.parseInt(cliParser.getOptionValue(GPUS, "0"));
    numWorkers = Integer.parseInt(cliParser.getOptionValue(WORKERS, "1"));
    numPses = Integer.parseInt(cliParser.getOptionValue(PSES, "1"));
    
    if (!(memory > 0 && vcores > 0 && gpus > -1 &&
        ((numWorkers > 0 && numPses > 0) || (numWorkers == 1 && numPses == 0)))) {
      throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores/gpus specified,"
          + " exiting."
          + " Specified memory=" + memory
          + ", vcores=" + vcores
          + ", gpus=" + gpus
          + ", numWorkers=" + numWorkers
          + ", numPses=" + numPses);
    }
  
    if (cliParser.hasOption(TENSORBOARD)) {
      tensorboard = true;
    }
    
    nodeLabelExpression = cliParser.getOptionValue(NODE_LABEL_EXPRESSION, null);
    
    clientTimeout = Long.parseLong(cliParser.getOptionValue(APPLICATION_TIMEOUT, "3600")) * 1000;
    
    attemptFailuresValidityInterval = Long.parseLong(
        cliParser.getOptionValue(ATTEMPT_FAILURES_VALIDITY_INTERVAL, "-1"));
    
    log4jPropFile = cliParser.getOptionValue(LOG_PROPERTIES, "");
    
    // Get timeline domain options
    if (cliParser.hasOption(DOMAIN)) {
      domainId = cliParser.getOptionValue(DOMAIN);
      toCreateDomain = cliParser.hasOption(CREATE);
      if (cliParser.hasOption(VIEW_ACLS)) {
        viewACLs = cliParser.getOptionValue(VIEW_ACLS);
      }
      if (cliParser.hasOption(MODIFY_ACLS)) {
        modifyACLs = cliParser.getOptionValue(MODIFY_ACLS);
      }
    }
    
    return true;
  }
  
  /**
   * Main run function for the client
   *
   * @return true if application completed successfully
   * @throws IOException
   * @throws YarnException
   */
  public boolean run() throws IOException, YarnException {
    // Submit and monitor the application
    return monitorApplication(submitApplication());
  }
  
  public ApplicationId submitApplication() throws IOException, YarnException {
    LOG.info("Running Client");
    yarnClient.start();
    
    logClusterState();
    
    if (domainId != null && domainId.length() > 0 && toCreateDomain) {
      prepareTimelineDomain();
    }
    
    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    ApplicationId appId = appResponse.getApplicationId();
    LOG.info("Received application id: " + appId);
    
    verifyClusterResources(appResponse);
    
    ContainerLaunchContext containerContext = createContainerLaunchContext(appResponse);
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(app, containerContext);
    
    LOG.info("Submitting application to ASM");
    yarnClient.submitApplication(appContext);
    
    return appId;
  }
  
  private void logClusterState() throws IOException, YarnException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM"
        + "\n\t numNodeManagers=" + clusterMetrics.getNumNodeManagers());
    
    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.info("Got node report from ASM for"
          + "\n\t nodeId=" + node.getNodeId()
          + "\n\t nodeAddress" + node.getHttpAddress()
          + "\n\t nodeRackName" + node.getRackName()
          + "\n\t nodeNumContainers" + node.getNumContainers());
    }
    
    QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
    LOG.info("Queue info"
        + "\n\t queueName=" + queueInfo.getQueueName()
        + "\n\t queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + "\n\t queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + "\n\t queueApplicationCount=" + queueInfo.getApplications().size()
        + "\n\t queueChildQueueCount=" + queueInfo.getChildQueues().size());
    
    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + "\n\t queueName=" + aclInfo.getQueueName()
            + "\n\t userAcl=" + userAcl.name());
      }
    }
  }
  
  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   *
   * @param appId
   *     Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  public boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
    
    YarnApplicationState oldState = null;
    
    while (true) {
      
      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }
      
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      
      if (oldState != state) {
        LOG.info("Got application report from ASM for"
            + "\n\t appId=" + appId.getId()
            + "\n\t clientToAMToken=" + report.getClientToAMToken()
            + "\n\t appDiagnostics=" + report.getDiagnostics()
            + "\n\t appMasterHost=" + report.getHost()
            + "\n\t appQueue=" + report.getQueue()
            + "\n\t appMasterRpcPort=" + report.getRpcPort()
            + "\n\t appStartTime=" + report.getStartTime()
            + "\n\t yarnAppState=" + report.getYarnApplicationState().toString()
            + "\n\t distributedFinalState=" + report.getFinalApplicationStatus().toString()
            + "\n\t appTrackingUrl=" + report.getTrackingUrl()
            + "\n\t appUser=" + report.getUser());
        oldState = state;
      } else {
        LOG.info("Got application report from ASM for " + appId + " (state: " + state + ")");
      }
      
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish."
            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
            + ". Breaking monitoring loop");
        return false;
      }
      
      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return false;
      }
    }
  }
  
  /**
   * Kill a submitted application by sending a call to the ASM
   *
   * @param appId
   *     Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  public void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
    yarnClient.killApplication(appId);
  }
  
  private void prepareTimelineDomain() {
    TimelineClient timelineClient;
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
      timelineClient.start();
    } else {
      LOG.warn("Cannot put the domain " + domainId +
          " because the timeline service is not enabled");
      return;
    }
    try {
      TimelineDomain domain = new TimelineDomain();
      domain.setId(domainId);
      domain.setReaders(viewACLs != null && viewACLs.length() > 0 ? viewACLs : " ");
      domain.setWriters(modifyACLs != null && modifyACLs.length() > 0 ? modifyACLs : " ");
      timelineClient.putDomain(domain);
      LOG.info("Put the timeline domain: " + TimelineUtils.dumpTimelineRecordtoJSON(domain));
    } catch (Exception e) {
      LOG.error("Error when putting the timeline domain", e);
    } finally {
      timelineClient.stop();
    }
  }
  
  private void verifyClusterResources(GetNewApplicationResponse appResponse) {
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
    if (amMemory > maxMem) {
      LOG.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + amMemory
          + ", max=" + maxMem);
      amMemory = maxMem;
    }
    
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);
    
    if (amVCores > maxVCores) {
      LOG.info("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
      amVCores = maxVCores;
    }
  }
  
  private ContainerLaunchContext createContainerLaunchContext(GetNewApplicationResponse appResponse)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    ApplicationId appId = appResponse.getApplicationId();
    
    DistributedCacheList dcl = populateDistributedCache(fs, appId);
    Map<String, LocalResource> localResources = prepareLocalResources(fs, appId, dcl);
    Map<String, String> launchEnv = setupLaunchEnv();
    
    // Set the executable command for the application master
    Vector<CharSequence> vargs = new Vector<>(30);
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    vargs.add("-Xmx" + amMemory + "m");
    vargs.add(appMasterMainClass);
    
    vargs.add(newArg(MEMORY, String.valueOf(memory)));
    vargs.add(newArg(VCORES, String.valueOf(vcores)));
    vargs.add(newArg(GPUS, String.valueOf(gpus)));
    // vargs.add(newArg(PRIORITY, String.valueOf(priority)));
    vargs.add(newArg(ALLOCATION_TIMEOUT, String.valueOf(allocationTimeout / 1000)));
    
    vargs.add(newArg(ApplicationMasterArguments.MAIN_RELATIVE, mainRelativePath));
    if (arguments != null) {
      vargs.add(newArg(ARGS, StringUtils.join(arguments, " ")));
    }
    vargs.add(newArg(WORKERS, Integer.toString(numWorkers)));
    vargs.add(newArg(PSES, Integer.toString(numPses)));
    
    for (Map.Entry<String, String> entry : environment.entrySet()) {
      vargs.add(newArg(ENV, entry.getKey() + "=" + entry.getValue()));
    }
    if (tensorboard) {
      vargs.add("--" + TENSORBOARD);
    }
    if (debugFlag) {
      vargs.add("--" + DEBUG);
    }
    
    // Add log redirect params
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
    
    // Get final command
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    
    LOG.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());
    
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, launchEnv, commands, null, null, null);
    
    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }
      // For now: only getting tokens for the default file-system.
      final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }
    
    return amContainer;
  }
  
  private DistributedCacheList populateDistributedCache(FileSystem fs, ApplicationId appId) throws IOException {
    DistributedCacheList distCacheList = new DistributedCacheList();
    
    mainRelativePath = addResource(fs, appId, mainPath, null, null, distCacheList, null, null);
    
    StringBuilder pythonPath = new StringBuilder(Constants.LOCALIZED_PYTHON_DIR);
    if (cliParser.hasOption(FILES)) {
      String[] files = cliParser.getOptionValue(FILES).split(",");
      for (String file : files) {
        if (file.endsWith(".py")) {
          addResource(fs, appId, file, Constants.LOCALIZED_PYTHON_DIR, null, distCacheList, null, null);
        } else {
          addResource(fs, appId, file, null, null, distCacheList, null, pythonPath);
        }
      }
    }
    environment.put("PYTHONPATH", pythonPath.toString());
    
    return distCacheList;
  }
  
  private Map<String, LocalResource> prepareLocalResources(FileSystem fs, ApplicationId appId, DistributedCacheList dcl)
      throws IOException {
    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<>();
    
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    addResource(fs, appId, amJar, null, Constants.AM_JAR_PATH, null, localResources, null);
    
    if (!log4jPropFile.isEmpty()) {
      addResource(fs, appId, log4jPropFile, null, Constants.LOG4J_PATH, null, localResources, null);
    }
    
    // Write distCacheList to HDFS and add to localResources
    Path baseDir = new Path(fs.getHomeDirectory(), Constants.YARNTF_STAGING + "/" + appId.toString());
    Path dclPath = new Path(baseDir, Constants.DIST_CACHE_PATH);
    FSDataOutputStream ostream = null;
    try {
      ostream = fs.create(dclPath);
      ostream.write(SerializationUtils.serialize(dcl));
    } finally {
      IOUtils.closeQuietly(ostream);
    }
    FileStatus dclStatus = fs.getFileStatus(dclPath);
    LocalResource distCacheResource = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(dclPath.toUri()),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        dclStatus.getLen(),
        dclStatus.getModificationTime());
    localResources.put(Constants.DIST_CACHE_PATH, distCacheResource);
    
    return localResources;
  }
  
  private String addResource(FileSystem fs, ApplicationId appId, String srcPath, String dstDir, String dstName,
      DistributedCacheList distCache, Map<String, LocalResource> localResources, StringBuilder pythonPath) throws
      IOException {
    Path src = new Path(srcPath);
    
    if (dstDir == null) {
      dstDir = ".";
    }
    if (dstName == null) {
      dstName = src.getName();
    }
    
    Path baseDir = new Path(fs.getHomeDirectory(), Constants.YARNTF_STAGING + "/" + appId.toString());
    String dstPath;
    if (dstDir.startsWith(".")) {
      dstPath = dstName;
    } else {
      dstPath = dstDir + "/" + dstName;
    }
    Path dst = new Path(baseDir, dstPath);
    
    LOG.info("Copying from local filesystem: " + src + " -> " + dst);
    fs.copyFromLocalFile(src, dst);
    FileStatus dstStatus = fs.getFileStatus(dst);
    
    if (distCache != null) {
      LOG.info("Adding to distributed cache: " + srcPath + " -> " + dstPath);
      distCache.add(new DistributedCacheList.Entry(
          dstPath, dst.toUri(), dstStatus.getLen(), dstStatus.getModificationTime()));
    }
    
    if (localResources != null) {
      LOG.info("Adding to local environment: " + srcPath + " -> " + dstPath);
      LocalResource resource = LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromURI(dst.toUri()),
          LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION,
          dstStatus.getLen(),
          dstStatus.getModificationTime());
      localResources.put(dstPath, resource);
    }
    
    if (pythonPath != null) {
      pythonPath.append(File.pathSeparator).append(dstPath);
    }
    
    return dstName;
  }
  
  private Map<String, String> setupLaunchEnv() throws IOException {
    LOG.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<>();
    
    if (domainId != null && domainId.length() > 0) {
      env.put(Constants.YARNTFTIMELINEDOMAIN, domainId);
    }
    
    // Add AppMaster.jar location to classpath
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
        "./log4j.properties");
    
    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    
    env.put("CLASSPATH", classPathEnv.toString());
    
    return env;
  }
  
  private String newArg(String param, String value) {
    return "--" + param + " " + value;
  }
  
  private ApplicationSubmissionContext createApplicationSubmissionContext(
      YarnClientApplication app, ContainerLaunchContext containerContext) {
    
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    
    if (name == null) {
      appContext.setApplicationName(mainRelativePath);
    } else {
      appContext.setApplicationName(name);
    }
    appContext.setApplicationType("YARNTF");
    
    appContext.setMaxAppAttempts(maxAppAttempts);
    appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
    appContext.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    
    if (null != nodeLabelExpression) {
      appContext.setNodeLabelExpression(nodeLabelExpression);
    }
    
    appContext.setResource(Resource.newInstance(amMemory, amVCores));
    appContext.setAMContainerSpec(containerContext);
    appContext.setPriority(Priority.newInstance(amPriority));
    appContext.setQueue(amQueue); // the queue to which this application is to be submitted in the RM
    
    return appContext;
  }
}
