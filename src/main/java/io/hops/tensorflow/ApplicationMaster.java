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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hops.tensorflow.ApplicationMasterArguments.APP_ATTEMPT_ID;
import static io.hops.tensorflow.ApplicationMasterArguments.DEBUG;
import static io.hops.tensorflow.ApplicationMasterArguments.ENV;
import static io.hops.tensorflow.ApplicationMasterArguments.HELP;
import static io.hops.tensorflow.ApplicationMasterArguments.MAIN_RELATIVE;
import static io.hops.tensorflow.ApplicationMasterArguments.MEMORY;
import static io.hops.tensorflow.ApplicationMasterArguments.PSES;
import static io.hops.tensorflow.ApplicationMasterArguments.VCORES;
import static io.hops.tensorflow.ApplicationMasterArguments.WORKERS;
import static io.hops.tensorflow.ApplicationMasterArguments.createOptions;
import static io.hops.tensorflow.CommonArguments.ALLOCATION_TIMEOUT;
import static io.hops.tensorflow.CommonArguments.ARGS;
import static io.hops.tensorflow.CommonArguments.HDFS_USER;
import static io.hops.tensorflow.CommonArguments.GPUS;
import static io.hops.tensorflow.CommonArguments.PROTOCOL;
import static io.hops.tensorflow.CommonArguments.PYTHON;
import static io.hops.tensorflow.CommonArguments.TENSORBOARD;
import static io.hops.tensorflow.Constants.LOG4J_PATH;

import java.util.logging.Level;
import java.util.logging.Logger;

// import static io.hops.tensorflow.ApplicationMasterArguments.PRIORITY;
public class ApplicationMaster {

  private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());

  public enum YarntfEvent {
    YARNTF_APP_ATTEMPT_START,
    YARNTF_APP_ATTEMPT_END,
    YARNTF_CONTAINER_START,
    YARNTF_CONTAINER_END
  }

  public enum YarntfEntity {
    YARNTF_APP_ATTEMPT,
    YARNTF_CONTAINER
  }

  public enum YarntfTask {
    YARNTF_WORKER,
    YARNTF_PS
  }

  // Configuration
  private final Configuration conf;

  // Handles to communicate with the Resource Manager and Node Manager
  private RMWrapper rmWrapper;
  private NMWrapper nmWrapper;

  private ApplicationAttemptId appAttemptID; // combination of attemptId and fail count
  private UserGroupInformation appSubmitterUgi;

  private String appMasterHostname;
  private final int appMasterRpcPort = -1;
  private String appMasterTrackingUrl = "";

  // App Master configuration
  private String containerPython;
  private int numWorkers;
  private int numPses;
  private int numTotalContainers;
  private int containerMemory;
  private int containerVirtualCores;
  private int containerGPUs;
  private String tfProtocol;
  // private int requestPriority;

  //Hopsworks variables
//  private String hdfsUser;

  // Timeout threshold for container allocation
  private final long appMasterStartTime = System.currentTimeMillis();
  private long allocationTimeout;

  // Counters for containers
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  private final AtomicInteger numCompletedWorkers = new AtomicInteger();
  private final AtomicInteger numAllocatedContainers = new AtomicInteger(); // by RM
  private final AtomicInteger numFailedContainers = new AtomicInteger();
  private final AtomicInteger numRequestedContainers = new AtomicInteger();

  // TF application
  private String mainRelative;
  private String[] arguments = new String[]{};
  private final Map<String, String> environment = new HashMap<>(); // Env variables
  private final Map<String, LocalResource> localResources = new HashMap<>();
  private ByteBuffer allTokens;

  private volatile boolean done;
  private final List<Thread> launchThreads = new ArrayList<>();

  private TimelineHandler timelineHandler;
  private String domainId; // Timeline domain ID

  private CommandLine cliParser;

  private ClusterSpecGeneratorServer clusterSpecServer;

  /**
   * @param args
   * Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  public ApplicationMaster() {
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options
   *
   * @param args
   * Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {
    Options opts = createOptions();
    cliParser = new GnuParser().parse(opts, args);

    containerPython = cliParser.getOptionValue(PYTHON, null);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }

    //Check whether customer log4j.properties file exists
    if (fileExist(LOG4J_PATH)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, LOG4J_PATH);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Can not set up custom log4j properties. {0}", e);
      }
    }

    if (cliParser.hasOption(HELP)) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption(DEBUG)) {
      dumpOutDebugInfo();
    }

    if (!cliParser.hasOption(MAIN_RELATIVE)) {
      throw new IllegalArgumentException("No main application file specified");
    }
    mainRelative = cliParser.getOptionValue(MAIN_RELATIVE);

    if (cliParser.hasOption(ARGS)) {
      arguments = cliParser.getOptionValues(ARGS);
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption(APP_ATTEMPT_ID)) {
        String appIdStr = cliParser.getOptionValue(APP_ATTEMPT_ID, "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name()
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name()
          + " not set in the environment");
    }

    LOG.log(Level.INFO, "Application master for app, appId={0}, clustertimestamp={1}, attemptId={2}", new Object[]{
      appAttemptID.getApplicationId().getId(),
      appAttemptID.getApplicationId().getClusterTimestamp(), appAttemptID.getAttemptId()});

    if (cliParser.hasOption(ENV)) {
      String shellEnvs[] = cliParser.getOptionValues(ENV);
      for (String env : shellEnvs) {
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

    if (cliParser.hasOption(TENSORBOARD)) {
      environment.put("YARNTF_TENSORBOARD", "true");
    }

    if (envs.containsKey(Constants.YARNTFTIMELINEDOMAIN)) {
      domainId = envs.get(Constants.YARNTFTIMELINEDOMAIN);
    }

    containerMemory = Integer.parseInt(cliParser.getOptionValue(MEMORY, "1024"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(VCORES, "1"));
    containerGPUs = Integer.parseInt(cliParser.getOptionValue(GPUS, "0"));
    tfProtocol = cliParser.getOptionValue(PROTOCOL, null);

    numWorkers = Integer.parseInt(cliParser.getOptionValue(WORKERS, "1"));
    numPses = Integer.parseInt(cliParser.getOptionValue(PSES, "1"));
    numTotalContainers = numWorkers + numPses;
    if (!(numWorkers > 0 && numPses > 0 || numWorkers == 1 && numPses == 0)) {
      throw new IllegalArgumentException("Invalid no. of workers or parameter server");
    }
    // requestPriority = Integer.parseInt(cliParser.getOptionValue(PRIORITY, "0"));

    allocationTimeout = Long.parseLong(cliParser.getOptionValue(ALLOCATION_TIMEOUT, "15")) * 1000;

    environment.put("YARNTF_MEMORY", Integer.toString(containerMemory));
    environment.put("YARNTF_VCORES", Integer.toString(containerVirtualCores));
    environment.put("YARNTF_GPUS", Integer.toString(containerGPUs));
    if (tfProtocol != null) {
      environment.put("YARNTF_PROTOCOL", tfProtocol);
    }
    environment.put("YARNTF_WORKERS", Integer.toString(numWorkers));
    environment.put("YARNTF_PSES", Integer.toString(numPses));
    //environment.put("YARNTF_HOME_DIR", FileSystem.get(conf).getHomeDirectory().toString());
    environment.put("PYTHONUNBUFFERED", "true");

    DistributedCacheList distCacheList = null;
    FileInputStream fin = null;
    ObjectInputStream ois = null;
    try {
      fin = new FileInputStream(Constants.DIST_CACHE_PATH);
      ois = new ObjectInputStream(fin);
      try {
        distCacheList = (DistributedCacheList) ois.readObject();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ois);
      org.apache.commons.io.IOUtils.closeQuietly(fin);
    }
    LOG.info("Loaded distribute cache list: " + distCacheList.toString());
    for (int i = 0; i < distCacheList.size(); i++) {
      DistributedCacheList.Entry entry = distCacheList.get(i);
      LocalResource distRsrc = LocalResource.newInstance(
          ConverterUtils.getYarnUrlFromURI(entry.uri),
          LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION,
          entry.size,
          entry.timestamp);
      localResources.put(entry.relativePath, distRsrc);
    }

    return true;
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnException
   * @throws IOException
   */
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster. " + "Workers: " + numWorkers + ", Parameter servers: " + numPses);

    clusterSpecServer = new ClusterSpecGeneratorServer(
        appAttemptID.getApplicationId().toString(), numTotalContainers, numWorkers);
    LOG.info("Starting ClusterSpecGeneratorServer");
    int port = 2222;
    while (true) {
      try {
        clusterSpecServer.start(port);
        break;
      } catch (IOException e) {
        port++;
      }
    }
    environment.put("YARNTF_AM_ADDRESS", InetAddress.getLocalHost().getHostName() + ":" + port);
    environment.put("YARNTF_APPLICATION_ID", appAttemptID.getApplicationId().toString());

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials = UserGroupInformation.createRemoteUser(System.getenv(Environment.USER.name())).
        getCredentials();//UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.log(Level.INFO, token.toString());
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName = System.getenv(Environment.USER.name());
    appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    rmWrapper = new RMWrapper(this);
    rmWrapper.getClient().init(conf);
    rmWrapper.getClient().start();

    nmWrapper = new NMWrapper(this);
    nmWrapper.getClient().init(conf);
    nmWrapper.getClient().start();

    timelineHandler = new TimelineHandler(appAttemptID.toString(), domainId, appSubmitterUgi);
    timelineHandler.startClient(conf);
    if (timelineHandler.isClientNotNull()) {
      timelineHandler.publishApplicationAttemptEvent(YarntfEvent.YARNTF_APP_ATTEMPT_START);
    }

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    appMasterTrackingUrl = InetAddress.getLocalHost().getHostName() + ":" + TensorBoardServer.spawn(this);
    RegisterApplicationMasterResponse response = rmWrapper.getClient()
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.log(Level.INFO, "Max mem capabililty of resources in this cluster {0}", maxMem);

    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.log(Level.INFO, "Max vcores capabililty of resources in this cluster {0}", maxVCores);

    int maxGPUS = response.getMaximumResourceCapability().getGPUs();
    LOG.log(Level.INFO, "Max gpus capabililty of resources in this cluster {0}", maxGPUS);

    // A resource ask cannot exceed the max.
    if (containerMemory > maxMem) {
      LOG.log(Level.INFO,
          "Container memory specified above max threshold of cluster. Using max value., specified={0}, max={1}",
          new Object[]{containerMemory,
            maxMem});
      containerMemory = maxMem;
    }

    if (containerVirtualCores > maxVCores) {
      LOG.log(Level.INFO,
          "Container virtual cores specified above max threshold of cluster. Using max value., specified={0}, max={1}",
          new Object[]{containerVirtualCores,
            maxVCores});
      containerVirtualCores = maxVCores;
    }

    if (containerGPUs > maxGPUS) {
      LOG.log(Level.INFO,
          "Container gpus specified above max threshold of cluster. Using max value., specified={0}, max={1}",
          new Object[]{containerGPUs,
            maxGPUS});
      containerGPUs = maxGPUS;
    }

    List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
    LOG.log(Level.INFO, "{0} received {1} previous attempts'' running containers on AM registration.", new Object[]{
      appAttemptID,
      previousAMRunningContainers.size()});
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

    // Stop eventual containers from previous attempts
    for (Container prevContainer : previousAMRunningContainers) {
      LOG.log(Level.INFO, "Releasing YARN container {0}", prevContainer.getId());
      rmWrapper.getClient().releaseAssignedContainer(prevContainer.getId());
    }

    // Send request for containers to RM
    for (int i = 0; i < numWorkers; i++) {
      ContainerRequest workerRequest = setupContainerAskForRM(true);
      rmWrapper.getClient().addContainerRequest(workerRequest);
    }
    numRequestedContainers.addAndGet(numWorkers);
    for (int i = 0; i < numPses; i++) {
      ContainerRequest psRequest = setupContainerAskForRM(false);
      rmWrapper.getClient().addContainerRequest(psRequest);
    }
    numRequestedContainers.addAndGet(numPses);
  }

  public void setDone() {
    done = true;
  }

  public void addLaunchThread(Thread lt) {
    launchThreads.add(lt);
  }

  /**
   *
   * @param lcontainer
   * @param jobName
   * @param taskIndex
   * @return
   */
  LaunchContainerRunnable createLaunchContainerRunnable(Container lcontainer, String jobName, int taskIndex) {
    return new LaunchContainerRunnable(lcontainer, jobName, taskIndex);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @param worker
   * @return the setup ResourceRequest to be sent to RM
   */
  public ContainerRequest setupContainerAskForRM(boolean worker) {
    Priority pri = Priority.newInstance(1);

    // Set up resource type requirements
    Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);

    if (worker) {
      pri.setPriority(0); // worker: 0, ps: 1
      capability.setGPUs(containerGPUs);
    }

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.log(Level.INFO, "Requested container ask: {0}", request.toString());
    return request;
  }

  /**
   * Get a list of all TensorBoards on the format [ip:port].
   *
   * @return list of all TensorBoards, or null if not yet available
   */
  public List<String> getTensorBoardEndpoint() {
    return new ArrayList<>(clusterSpecServer.getTensorBoards().values());
  }

  // Getters for NM and RM wrappers
  TimelineHandler getTimelineHandler() {
    return timelineHandler;
  }

  AtomicInteger getNumCompletedContainers() {
    return numCompletedContainers;
  }

  AtomicInteger getNumCompletedWorkers() {
    return numCompletedWorkers;
  }

  AtomicInteger getNumFailedContainers() {
    return numFailedContainers;
  }

  ApplicationAttemptId getAppAttemptID() {
    return appAttemptID;
  }

  AtomicInteger getNumAllocatedContainers() {
    return numAllocatedContainers;
  }

  AtomicInteger getNumRequestedContainers() {
    return numRequestedContainers;
  }

  int getNumTotalContainers() {
    return numTotalContainers;
  }

  int getNumWorkers() {
    return numWorkers;
  }

  int getNumPses() {
    return numPses;
  }

  int getContainerGPUs() {
    return containerGPUs;
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {
    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.log(Level.INFO, "System env: key={0}, val={1}", new Object[]{env.getKey(), env.getValue()});
      System.out.println("System env: key=" + env.getKey() + ", val="
          + env.getValue());
    }

    BufferedReader buf;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") : Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line;
      while ((line = buf.readLine()) != null) {
        LOG.log(Level.INFO, "System CWD content: {0}", line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Requested container ask: {0}", e.getMessage());
    }
    /*
     * finally {
     * IOUtils.cleanup(LOG, buf);
     * }
     */
  }

  /**
   * Helper function to print usage
   *
   * @param opts
   * Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  private boolean finish() {
    // wait for completion. finish if any container fails
    while (!done && !(numCompletedWorkers.get() == numWorkers) && !(numFailedContainers.get() > 0)) {
      if (numAllocatedContainers.get() != numTotalContainers) {
        long timeLeft = appMasterStartTime + allocationTimeout - System.currentTimeMillis();
        LOG.log(Level.INFO, "Awaits container allocation, timeLeft={0}", timeLeft);
        if (timeLeft < 0) {
          LOG.log(Level.WARNING, "Container allocation timeout. Finish application attempt");
          break;
        }
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
      }
    }

    if (timelineHandler.isClientNotNull()) {
      timelineHandler.publishApplicationAttemptEvent(YarntfEvent.YARNTF_APP_ATTEMPT_END);
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Exception thrown in thread join: " + e.getMessage(), e);
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmWrapper.getClient().stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numFailedContainers.get() == 0 && numCompletedWorkers.get() == numWorkers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed(workers)="
          + numCompletedContainers.get() + "(" + numCompletedWorkers.get() + "), allocated="
          + numAllocatedContainers.get() + ", failed=" + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }
    try {
      rmWrapper.getClient().unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException | IOException ex) {
      LOG.log(Level.SEVERE, "Failed to unregister application", ex);
    }

    rmWrapper.getClient().stop();

    // Stop Timeline Client
    if (timelineHandler.isClientNotNull()) {
      timelineHandler.stopClient();
    }

    return success;
  }

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the Python application
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    Container container;

    String jobName;
    int taskIndex;

    public LaunchContainerRunnable(
        Container lcontainer, String jobName, int taskIndex) {
      this.container = lcontainer;
      this.jobName = jobName;
      this.taskIndex = taskIndex;
    }

    @Override
    /**
     * Connects to CM, sets up container launch context
     * for Python application and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
      LOG.log(Level.INFO, "Setting up container launch container for containerid={0}", container.getId());

      Map<String, String> envCopy = new HashMap<>(environment);
      envCopy.put("YARNTF_JOB_NAME", jobName);
//      envCopy.put("HADOOP_USER_NAME", hdfsUser);
      envCopy.put("YARNTF_TASK_INDEX", Integer.toString(taskIndex));
      if (jobName.equals("worker")) {
        envCopy.put("YARNTF_TB_DIR", "tensorboard_" + taskIndex);
      }

      // Set the executable command for the allocated container
      List<CharSequence> vargs = new ArrayList<>(15);

      // https://www.tensorflow.org/deploy/hadoop
      // https://www.tensorflow.org/install/install_linux
      if (envCopy.containsKey("CUDA_HOME")) {
        vargs.add("CUDA_HOME=" + envCopy.get("CUDA_HOME"));
      } else {
        vargs.add("CUDA_HOME=/usr/local/cuda");
      }
      vargs.add("LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server:$CUDA_HOME/lib64");
      vargs.add("PATH=$PATH:$CUDA_HOME/bin");
      vargs.add("CLASSPATH=$($HADOOP_HDFS_HOME/bin/hadoop classpath --glob)");

      if (containerPython != null) {
        LOG.log(Level.INFO, "Using custom Python binary: {0}", containerPython);
        vargs.add(containerPython + " " + mainRelative);
      } else {
        vargs.add("python " + mainRelative);
      }
      vargs.add(StringUtils.join(arguments, " "));

      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      // Get final command
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<>();
      commands.add(command.toString());

      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
          localResources, envCopy, commands, null, allTokens.duplicate(), null);

      nmWrapper.addContainer(container.getId(), container, jobName.equals("worker"));
      nmWrapper.getClient().startContainerAsync(container, ctx);
    }
  }
}
