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

import org.apache.commons.cli.Options;

public class ClientArguments extends CommonArguments {
  
  public static final String NAME = "name";
  public static final String MAIN = "main";
  public static final String FILES = "files";
  
  public static final String AM_JAR = "am_jar";
  public static final String AM_MEMORY = "am_memory";
  public static final String AM_VCORES = "am_vcores";
  public static final String AM_PRIORITY = "am_priority";
  
  public static final String QUEUE = "queue";
  public static final String APPLICATION_TIMEOUT = "application_timeout";
  public static final String LOG_PROPERTIES = "log_properties";
  
  public static final String MAX_APP_ATTEMPTS = "max_app_attempts";
  public static final String KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS =
      "keep_containers_across_application_attempts";
  public static final String ATTEMPT_FAILURES_VALIDITY_INTERVAL = "attempt_failures_validity_interval";
  public static final String DOMAIN = "domain";
  public static final String VIEW_ACLS = "view_acls";
  public static final String MODIFY_ACLS = "modify_acls";
  public static final String CREATE = "create";
  public static final String NODE_LABEL_EXPRESSION = "node_label_expression";
  
  
  public static Options createOptions() {
    Options opts = CommonArguments.createOptions();
    opts.addOption(NAME, true, "A name of your application.");
    opts.addOption(MAIN, true, "Your application's main Python file.");
    opts.addOption(FILES, true,
        "Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.");
    
    opts.addOption(AM_JAR, true, "Jar file containing the application master");
    opts.addOption(AM_MEMORY, true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption(AM_VCORES, true, "Amount of virtual cores to be requested to run the application master");
    opts.addOption(AM_PRIORITY, true, "Application Master Priority. Default 0");
    
    opts.addOption(QUEUE, true, "RM Queue in which this application is to be submitted");
    opts.addOption(APPLICATION_TIMEOUT, true, "Application timeout in seconds");
    opts.addOption(LOG_PROPERTIES, true, "log4j.properties file");
    
    opts.addOption(MAX_APP_ATTEMPTS, true, "Number of max attempts of the application to be submitted");
    opts.addOption(KEEP_CONTAINERS_ACROSS_APPLICATION_ATTEMPTS, false,
        "Flag to indicate whether to keep containers across application attempts." +
            " If the flag is true, running containers will not be killed when" +
            " application attempt fails and these containers will be retrieved by" +
            " the new application attempt ");
    opts.addOption(ATTEMPT_FAILURES_VALIDITY_INTERVAL, true,
        "when attempt_failures_validity_interval in milliseconds is set to > 0," +
            "the failure number will not take failures which happen out of " +
            "the validityInterval into failure count. " +
            "If failure count reaches to maxAppAttempts, " +
            "the application will be failed.");
    opts.addOption(DOMAIN, true, "ID of the timeline domain where the "
        + "timeline entities will be put");
    opts.addOption(VIEW_ACLS, true, "Users and groups that allowed to "
        + "view the timeline entities in the given domain");
    opts.addOption(MODIFY_ACLS, true, "Users and groups that allowed to "
        + "modify the timeline entities in the given domain");
    opts.addOption(CREATE, false, "Flag to indicate whether to create the "
        + "domain specified with -domain.");
    opts.addOption(NODE_LABEL_EXPRESSION, true,
        "Node label expression to determine the nodes"
            + " where all the containers of this application"
            + " will be allocated, \"\" means containers"
            + " can be allocated anywhere, if you don't specify the option,"
            + " default node_label_expression of queue will be used.");
    return opts;
  }
}
