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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.Service;
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

import static io.hops.tensorflow.ClientArguments.AM_JAR;
import static io.hops.tensorflow.ClientArguments.AM_MEMORY;
import static io.hops.tensorflow.ClientArguments.AM_PRIORITY;
import static io.hops.tensorflow.ClientArguments.AM_VCORES;
import static io.hops.tensorflow.ClientArguments.APPLICATION_TIMEOUT;
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
import static io.hops.tensorflow.ClientArguments.PSES;
import static io.hops.tensorflow.ClientArguments.QUEUE;
import static io.hops.tensorflow.ClientArguments.VCORES;
import static io.hops.tensorflow.ClientArguments.VIEW_ACLS;
import static io.hops.tensorflow.ClientArguments.WORKERS;
import static io.hops.tensorflow.ClientArguments.createOptions;
import static io.hops.tensorflow.CommonArguments.ALLOCATION_TIMEOUT;
import static io.hops.tensorflow.CommonArguments.GPUS;
import static io.hops.tensorflow.CommonArguments.PROTOCOL;
import static io.hops.tensorflow.CommonArguments.PYTHON;
import static io.hops.tensorflow.CommonArguments.TENSORBOARD;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {

  private static final Logger LOG = Logger.getLogger(Client.class.getName());

  // Configuration
  private Configuration conf;
  private YarnClient yarnClient;

  // App master config
  private int amPriority;
  private String queue;
  private int amMemory;
  private int amVCores;
  private String amJar; // path
  private final String appMasterMainClass; // class name

  private List<String> files; // local resources in csv format
  private Map<String, LocalResourceInfo> filesInfo = new HashMap<>();
  private String projectDir;
  // TF application config
  private String python;
  // private int priority;
  private long allocationTimeout;
  private String name;
  private String main;
  private String mainRelative; // relative path for worker or ps
  private String[] arguments; // to be passed to the application
  private int numWorkers;
  private int numPses;
  private int memory;
  private int vcores;
  private int gpus;
  private String protocol;
  private final Map<String, String> environment = new HashMap<>(); // environment variables
  private final String APPID_REGEX = "\\$APPID";
  private boolean tensorboard;

  private String nodeLabelExpression;
  private String log4jPropFile; // if available, add to local resources and set into classpath

  // Timeout threshold for client. Kill app after time interval expires
  private final long clientStartTime = System.currentTimeMillis();
  private long clientTimeout;

  private int maxAppAttempts;
  private boolean keepContainers; // keep containers across application attempts.
  private long attemptFailuresValidityInterval;

  private boolean debug;

  // Timeline config
  private String domainId;
  private boolean toCreateDomain; // whether to create the domain of the given ID
  private String viewACLs; // reader access control
  private String modifyACLs; // writer access control

  // Command line options
  private Options opts;
  private CommandLine cliParser;
  private final Map<String, LocalResource> localResources = new HashMap<>();

  /**
   * @param args
   * Command line arguments
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
      LOG.log(Level.SEVERE, "Error running Client", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application completed successfully");
      System.exit(0);
    }
    LOG.log(Level.SEVERE, "Application failed to complete successfully");
    System.exit(2);
  }

  /**
   * @param conf
   * @throws java.lang.Exception
   */
  public Client(Configuration conf) throws Exception {
    this(ApplicationMaster.class.getName(), conf);
  }

  Client(String appMasterMainClass, Configuration conf) {
    this.appMasterMainClass = appMasterMainClass;
    this.conf = conf;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    opts = createOptions();
  }

  /**
   * @throws java.lang.Exception
   */
  public Client() throws Exception {
    this.appMasterMainClass = ApplicationMaster.class.getName();
  }

  /**
   * Helper function to print out usage
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }

  /**
   * Minimal init
   *
   * @param amJar
   * Path to JAR file containing the application master
   * @param main
   * Your application's main Python file
   * @param files
   * Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps
   * @param logProperties
   * log4j.properties file
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String amJar, String main, String files, String logProperties) throws ParseException {

    if (amJar == null || main == null) {
      throw new IllegalArgumentException("Null not allowed! amJar=" + amJar + ", main=" + main);
    }

    ArrayList<String> args = new ArrayList<>();

    args.add("--" + AM_JAR);
    args.add(amJar);

    args.add("--" + MAIN);
    args.add(main);

    if (files != null) {
      args.add("--" + FILES);
      args.add(files);
    }

    if (logProperties != null) {
      args.add("--" + LOG_PROPERTIES);
      args.add(logProperties);
    }

    return init(args.toArray(new String[0]));
  }

  /**
   * Parse command line options
   *
   * @param args
   * Parsed command line options
   * @return Whether the init was successful to run the client
   * @throws ParseException
   */
  public boolean init(String[] args) throws ParseException {

    cliParser = new GnuParser().parse(opts, args);

    python = cliParser.getOptionValue(PYTHON, null);

    if (args.length == 0) {
      printUsage();
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption(LOG_PROPERTIES)) {
      String log4jPath = cliParser.getOptionValue(LOG_PROPERTIES);
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Can not set up custom log4j properties. ", e);
      }
    }

    if (cliParser.hasOption(HELP)) {
      printUsage();
      return false;
    }

    if (cliParser.hasOption(DEBUG)) {
      debug = true;
    }

    maxAppAttempts = Integer.parseInt(cliParser.getOptionValue(MAX_APP_ATTEMPTS, "1"));

    if (cliParser.hasOption(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS)) {
      LOG.info(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS);
      keepContainers = true;
    }

    name = cliParser.getOptionValue(NAME);
    amPriority = Integer.parseInt(cliParser.getOptionValue(AM_PRIORITY, "0"));
    queue = cliParser.getOptionValue(QUEUE, "default");
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
    main = cliParser.getOptionValue(MAIN);

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
    //Used for RDMA
    protocol = cliParser.getOptionValue(PROTOCOL, null);

    numWorkers = Integer.parseInt(cliParser.getOptionValue(WORKERS, "1"));
    numPses = Integer.parseInt(cliParser.getOptionValue(PSES, "1"));

    if (!(memory > 0 && vcores > 0 && gpus > -1
        && ((numWorkers > 0 && numPses > 0) || (numWorkers == 1 && numPses == 0)))) {
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

    //Used for RDMA
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
    YarnClientApplication app = createApplication();
    submitApplication(app);
    return monitorApplication(app.getNewApplicationResponse().getApplicationId());
  }

  public YarnClientApplication createApplication() throws YarnException, IOException{
    LOG.info("Running Client");
    
    if (null == yarnClient
        || !yarnClient.getServiceState().equals(Service.STATE.STARTED)) {
      LOG.severe("YarnClient is not started, state is: " + yarnClient
          .getServiceState().toString());
      throw new YarnException("YarnClient is not started <" + yarnClient
          .getServiceState().toString() + ">");
    }
    
    return yarnClient.createApplication();
  }
  
  public void submitApplication(YarnClientApplication app) throws
      IOException, YarnException {
    
    if (null == app) {
      throw new YarnException("YarnClientApplication is null. Have called " +
          "createApplication?");
    }

    logClusterState();

    if (domainId != null && domainId.length() > 0 && toCreateDomain) {
      prepareTimelineDomain();
    }

    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    verifyClusterResources(appResponse);

    ContainerLaunchContext containerContext = createContainerLaunchContext(appResponse);
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(app, containerContext);

    LOG.info("Submitting application to ASM");
    yarnClient.submitApplication(appContext);
  }
  
  /**
   * @deprecated This method is deprecated. You should use createApplication()
   * and submitApplication(YarnClientApplication) instead
   * @return
   * @throws IOException
   * @throws YarnException
   */
  @Deprecated
  public ApplicationId submitApplication() throws IOException, YarnException {
    LOG.info("Running Client");
    if (!yarnClient.getServiceState().equals(Service.STATE.STARTED)) {
      yarnClient.start();
    }
    
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
    LOG.log(Level.INFO, "Got Cluster metric info from ASM\n\t numNodeManagers={0}", clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.log(Level.INFO,
          "Got node report from ASM for\n\t nodeId={0}\n\t nodeAddress{1}\n\t nodeRackName{2}\n\t nodeNumContainers{3}",
          new Object[]{node.getNodeId(),
            node.getHttpAddress(), node.getRackName(), node.getNumContainers()});
    }

    QueueInfo queueInfo = yarnClient.getQueueInfo(this.queue);
    LOG.log(Level.INFO,
        "Queue info\n\t queueName={0}\n\t queueCurrentCapacity={1}\n\t queueMaxCapacity={2}\n\t "
        + "queueApplicationCount={3}\n\t queueChildQueueCount={4}",
        new Object[]{queueInfo.getQueueName(),
          queueInfo.getCurrentCapacity(), queueInfo.getMaximumCapacity(), queueInfo.getApplications().size(),
          queueInfo.getChildQueues().size()});

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.log(Level.INFO, "User ACL Info for Queue\n\t queueName={0}\n\t userAcl={1}", new Object[]{aclInfo.
          getQueueName(),
          userAcl.name()});
      }
    }
  }

  /**
   * Monitor the submitted application for completion.
   * Kill application if time expires.
   *
   * @param appId
   * Application Id of application to be monitored
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
        LOG.log(Level.FINE, "Thread sleep in monitoring loop interrupted");
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
        LOG.log(Level.INFO, "Got application report from ASM for {0} (state: {1})", new Object[]{appId, state});
      }

      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.log(Level.INFO,
              "Application did finished unsuccessfully. YarnState={0}, DSFinalStatus={1}. Breaking monitoring loop",
              new Object[]{state.toString(),
                dsStatus.toString()});
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.log(Level.INFO, "Application did not finish. YarnState={0}, DSFinalStatus={1}. Breaking monitoring loop",
            new Object[]{state.toString(),
              dsStatus.toString()});
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
   * Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  public void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
    yarnClient.killApplication(appId);
  }

  // Getters and setters for HopsWorks, see ClientArguments and CommonArguments for variable description
  public int getAmPriority() {
    return amPriority;
  }

  public void setAmPriority(int amPriority) {
    this.amPriority = amPriority;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public int getAmMemory() {
    return amMemory;
  }

  public void setAmMemory(int amMemory) {
    this.amMemory = amMemory;
  }

  public int getAmVCores() {
    return amVCores;
  }

  public void setAmVCores(int amVCores) {
    this.amVCores = amVCores;
  }

  public String getAmJar() {
    return amJar;
  }

  public void setAmJar(String amJar) {
    this.amJar = amJar;
  }

  public String getPython() {
    return python;
  }

  public void setPython(String python) {
    this.python = python;
  }

  public long getAllocationTimeout() {
    return allocationTimeout;
  }

  public void setAllocationTimeout(long allocationTimeout) {
    this.allocationTimeout = allocationTimeout;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getMain() {
    return main;
  }

  public void setMain(String main) {
    this.main = main;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setArguments(String[] arguments) {
    this.arguments = arguments;
  }

  public List<String> getFiles() {
    return files;
  }

  public void setFiles(List<String> files) {
    this.files = files;
  }

  public void addFile(String file) {
    if (this.files == null) {
      this.files = new ArrayList<>();
    }
    this.files.add(file);
  }

  public Map<String, LocalResourceInfo> getFilesInfo() {
    return filesInfo;
  }

  public void setFilesInfo(Map<String, LocalResourceInfo> filesInfo) {
    this.filesInfo = filesInfo;
  }

  public void setProjectDir(String projectDir) {
    this.projectDir = projectDir;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public void setNumWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
  }

  public int getNumPses() {
    return numPses;
  }

  public void setNumPses(int numPses) {
    this.numPses = numPses;
  }

  public int getMemory() {
    return memory;
  }

  public void setMemory(int memory) {
    this.memory = memory;
  }

  public int getVcores() {
    return vcores;
  }

  public void setVcores(int vcores) {
    this.vcores = vcores;
  }

  public int getGpus() {
    return gpus;
  }

  public void setGpus(int gpus) {
    this.gpus = gpus;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public void addEnvironmentVariable(String key, String val) {
    this.environment.put(key, val);
  }

  public boolean isTensorboard() {
    return tensorboard;
  }

  public void setTensorboard(boolean tensorboard) {
    this.tensorboard = tensorboard;
  }

  public String getNodeLabelExpression() {
    return nodeLabelExpression;
  }

  public void setNodeLabelExpression(String nodeLabelExpression) {
    this.nodeLabelExpression = nodeLabelExpression;
  }

  public String getLog4jPropFile() {
    return log4jPropFile;
  }

  public void setLog4jPropFile(String log4jPropFile) {
    this.log4jPropFile = log4jPropFile;
  }

  public long getClientTimeout() {
    return clientTimeout;
  }

  public void setClientTimeout(long clientTimeout) {
    this.clientTimeout = clientTimeout;
  }

  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public void setMaxAppAttempts(int maxAppAttempts) {
    this.maxAppAttempts = maxAppAttempts;
  }

  public boolean isKeepContainers() {
    return keepContainers;
  }

  public void setKeepContainers(boolean keepContainers) {
    this.keepContainers = keepContainers;
  }

  public long getAttemptFailuresValidityInterval() {
    return attemptFailuresValidityInterval;
  }

  public void setAttemptFailuresValidityInterval(long attemptFailuresValidityInterval) {
    this.attemptFailuresValidityInterval = attemptFailuresValidityInterval;
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public String getDomainId() {
    return domainId;
  }

  public void setDomainId(String domainId) {
    this.domainId = domainId;
  }

  public boolean isToCreateDomain() {
    return toCreateDomain;
  }

  public void setToCreateDomain(boolean toCreateDomain) {
    this.toCreateDomain = toCreateDomain;
  }

  public String getViewACLs() {
    return viewACLs;
  }

  public void setViewACLs(String viewACLs) {
    this.viewACLs = viewACLs;
  }

  public String getModifyACLs() {
    return modifyACLs;
  }

  public void setModifyACLs(String modifyACLs) {
    this.modifyACLs = modifyACLs;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void initYarnClient() {
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    opts = createOptions();
  }

  private void prepareTimelineDomain() {
    TimelineClient timelineClient;
    LOG.log(Level.INFO, "prepareTimelineDomain Timeline defaultFS:{0}", conf.get("fs.defaultFS"));
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
      timelineClient.start();
    } else {
      LOG.log(Level.WARNING, "Cannot put the domain {0} because the timeline service is not enabled", domainId);
      return;
    }
    try {
      TimelineDomain domain = new TimelineDomain();
      domain.setId(domainId);
      domain.setReaders(viewACLs != null && viewACLs.length() > 0 ? viewACLs : " ");
      domain.setWriters(modifyACLs != null && modifyACLs.length() > 0 ? modifyACLs : " ");
      timelineClient.putDomain(domain);
      LOG.log(Level.INFO, "Put the timeline domain: {0}", TimelineUtils.dumpTimelineRecordtoJSON(domain));
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error when putting the timeline domain", e);
    } finally {
      timelineClient.stop();
    }
  }

  private void verifyClusterResources(GetNewApplicationResponse appResponse) {
    int maxMem = appResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    if (amMemory > maxMem) {
      LOG.
          log(Level.INFO, "AM memory specified above max threshold of cluster. Using max value., specified={0}, max={1}",
              new Object[]{amMemory,
                maxMem});
      amMemory = maxMem;
    }

    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.log(Level.INFO, "Max virtual cores capabililty of resources in this cluster {0}", maxVCores);

    if (amVCores > maxVCores) {
      LOG.log(Level.INFO,
          "AM virtual cores specified above max threshold of cluster. Using max value., specified={0}, max={1}",
          new Object[]{amVCores,
            maxVCores});
      amVCores = maxVCores;
    }
  }

  private ContainerLaunchContext createContainerLaunchContext(GetNewApplicationResponse appResponse)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    ApplicationId appId = appResponse.getApplicationId();

    DistributedCacheList dcl = populateDistributedCache(fs, appId);
    localResources.putAll(prepareLocalResources(fs, appId, dcl));
    Map<String, String> launchEnv = setupLaunchEnv();

    // Set the executable command for the application master
    List<CharSequence> vargs = new ArrayList<>(30);
    LOG.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    vargs.add("-Xmx" + amMemory + "m");
    vargs.add(appMasterMainClass);

    if (python != null) {
      vargs.add(newArg(PYTHON, python));
    }
    vargs.add(newArg(MEMORY, String.valueOf(memory)));
    vargs.add(newArg(VCORES, String.valueOf(vcores)));
    vargs.add(newArg(GPUS, String.valueOf(gpus)));
    if (protocol != null) {
      vargs.add(newArg(PROTOCOL, protocol));
    }
    // vargs.add(newArg(PRIORITY, String.valueOf(priority)));
    vargs.add(newArg(ALLOCATION_TIMEOUT, String.valueOf(allocationTimeout / 1000)));

    vargs.add(newArg(ApplicationMasterArguments.MAIN_RELATIVE, mainRelative));
    if (arguments != null) {
      vargs.add(newArg(ARGS, StringUtils.join(arguments, " ")));
    }
    vargs.add(newArg(WORKERS, Integer.toString(numWorkers)));
    vargs.add(newArg(PSES, Integer.toString(numPses)));

    for (Map.Entry<String, String> entry : environment.entrySet()) {
      //Replace AppId regex
      vargs.add(newArg(ENV, entry.getKey() + "=" + entry.getValue().replaceAll(APPID_REGEX, appId.toString())));
    }
    if (tensorboard) {
      vargs.add("--" + TENSORBOARD);
    }
    if (debug) {
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

    LOG.log(Level.INFO, "Completed setting up app master command {0}", command.toString());
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
          LOG.log(Level.FINE, "Got dt for {0}; {1}", new Object[]{fs.getUri(), token});
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

    mainRelative = addResource(fs, appId, main, null, null, distCacheList, null, null, null, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION, null);

    StringBuilder pythonPath = new StringBuilder(Constants.LOCALIZED_PYTHON_DIR);
    if (files == null || files.isEmpty()) {
      if (cliParser != null && cliParser.hasOption(FILES)) {
        files = Arrays.asList(cliParser.getOptionValue(FILES).split(","));
      }
    } else {
      LOG.log(Level.FINE, "TF files:{0}", Arrays.toString(files.toArray()));
      for (String file : files) {
        if (file.endsWith(".py")) {
          addResource(fs, appId, file, Constants.LOCALIZED_PYTHON_DIR, null, distCacheList, localResources, null,
              filesInfo.get(file).getName(), filesInfo.get(file).getType(), filesInfo.get(file).getVisibility(),
              filesInfo.get(file).getPattern());
        } else {
          addResource(fs, appId, file, null, null, distCacheList, localResources, pythonPath, filesInfo.get(
              file).getName(), filesInfo.get(file).getType(), filesInfo.get(file).getVisibility(), filesInfo.get(file).
              getPattern());
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

    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    addResource(fs, appId, amJar, null, Constants.AM_JAR_PATH, null, localResources, null, null, LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION, null);

    if (log4jPropFile != null && !log4jPropFile.isEmpty()) {
      addResource(fs, appId, log4jPropFile, null, Constants.LOG4J_PATH, null, localResources, null, null,
          LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, null);
    }

    // Write distCacheList to HDFS and add to localResources
    Path baseDir = new Path(projectDir, "Resources/" + Constants.YARNTF_STAGING + "/" + appId.toString());
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
      DistributedCacheList distCache, Map<String, LocalResource> localResources, StringBuilder pythonPath,
      String localResourceName, LocalResourceType type, LocalResourceVisibility visibility, String pattern)
      throws IOException {
    Path src = new Path(srcPath);

    if (localResourceName != null) {
      dstName = localResourceName;
    } else if (dstName == null) {
      dstName = src.getName();
    }
    String dstPath;
    Path dst;
    if (srcPath.startsWith("hdfs://")) {
      // no copying, `dstDir` and `dstName` will be ignored
      dstPath = srcPath;
      dst = src;
      LOG.log(Level.INFO, "Copying from hdfs: {0} -> {1}", new Object[]{src, dst});
    } else {
      Path baseDir = new Path(fs.getHomeDirectory(), Constants.YARNTF_STAGING + "/" + appId.toString());
      if (dstDir == null) {
        dstDir = ".";
      }
      if (dstDir.startsWith(".")) {
        dstPath = dstName;
      } else {
        dstPath = dstDir + "/" + dstName;
      }
      dst = new Path(baseDir, dstPath);

      LOG.log(Level.INFO, "Copying from local filesystem: {0} -> {1}", new Object[]{src, dst});
      fs.copyFromLocalFile(src, dst);
    }

    FileStatus dstStatus = fs.getFileStatus(dst);

    if (distCache != null) {
      LOG.log(Level.INFO, "Adding to distributed cache: {0} -> {1}", new Object[]{srcPath, dstPath});
      distCache.add(new DistributedCacheList.Entry(
          dstName, dst.toUri(), dstStatus.getLen(), dstStatus.getModificationTime()));
    }

    if (localResources != null) {
      LOG.log(Level.INFO, "Adding to local environment: {0} -> {1}", new Object[]{srcPath, dstPath});
      LocalResource resource = null;
      if (pattern != null) {
        resource = LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            type,
            visibility,
            dstStatus.getLen(),
            dstStatus.getModificationTime(),
            pattern);
      } else {
        resource = LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            type,
            visibility,
            dstStatus.getLen(),
            dstStatus.getModificationTime());
      }
      localResources.put(dstName, resource);
    }

    if (pythonPath != null) {
      pythonPath.append(File.pathSeparator).append(dstName);
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
      appContext.setApplicationName(mainRelative);
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
    appContext.setQueue(queue); // the queue to which this application is to be submitted in the RM

    return appContext;
  }
}
