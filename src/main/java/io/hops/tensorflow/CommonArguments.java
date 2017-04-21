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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class CommonArguments {
  
  public static final String ARGS = "args";
  public static final String WORKERS = "workers";
  public static final String PSES = "pses";
  public static final String ENV = "env";
  public static final String TENSORBOARD = "tensorboard";
  
  public static final String MEMORY = "memory";
  public static final String VCORES = "vcores";
  public static final String GPUS = "gpus";
  // public static final String PRIORITY = "priority";
  public static final String ALLOCATION_TIMEOUT = "allocation_timeout";
  
  public static final String DEBUG = "debug";
  public static final String HELP = "help";
  
  protected static Options createOptions() {
    Options opts = new Options();
    
    opts.addOption(ARGS, true, "Command line args for the application. Multiple args can be separated by empty space.");
    opts.getOption(ARGS).setArgs(Option.UNLIMITED_VALUES);
    opts.addOption(WORKERS, true, "Number of workers");
    opts.addOption(PSES, true, "Number of parameter servers");
    opts.addOption(ENV, true, "Environment for Python application. Specified as env_key=env_val pairs");
    opts.addOption(TENSORBOARD, false, "Enable TensorBoard, one for each worker");
    
    opts.addOption(MEMORY, true, "Amount of memory in MB to be requested to run the TF application");
    opts.addOption(VCORES, true, "Amount of virtual cores to be requested to run the TF application");
    opts.addOption(GPUS, true, "Amount of GPUs for each TF application container");
    // opts.addOption(PRIORITY, true, "Priority for the Python application containers");
    opts.addOption(ALLOCATION_TIMEOUT, true, "Container allocation timeout in seconds");
    
    opts.addOption(DEBUG, false, "Dump out debug information");
    opts.addOption(HELP, false, "Print usage");
    return opts;
  }
}
