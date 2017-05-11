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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestPyModule {
  
  private ClusterSpecGeneratorServer server;
  
  @Before
  public void setup() {
    server = new ClusterSpecGeneratorServer("(appId)", 3, 3);
    int port = 50052;
    while (port <= 65535) {
      try {
        server.start(port);
        break;
      } catch (IOException e) {
        port++;
      }
    }
  }
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }
  
  @Test
  public void FactoryTest() throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String pyPath = classLoader.getResource("factory_test.py").getPath();
    Process p = Runtime.getRuntime().exec("python " + pyPath);
    p.waitFor();
    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    
    int regs = 0;
    String line = "";
    while ((line = reader.readLine())!= null) {
      System.out.println(line);
      if (line.contains("registered: True")) {
        regs += 1;
      }
    }
    Assert.assertEquals(3, regs);
    Assert.assertEquals(3, server.getClusterSpec().size());
    Assert.assertEquals(1, server.getTensorBoards().size());
  }
}
