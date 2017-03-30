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

public class ApplicationMasterArguments extends CommonArguments {
  
  public static final String APP_ATTEMPT_ID = "app_attempt_id";
  public static final String MAIN_RELATIVE = "main_relative";
  
  public static final Options createOptions() {
    Options opts = CommonArguments.createOptions();
    opts.addOption(APP_ATTEMPT_ID, true, "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption(MAIN_RELATIVE, true, "Your application's main Python file. Relative path for worker or ps");
    return opts;
  }
}
