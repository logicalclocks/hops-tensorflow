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

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.cli.LogsCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
  
  public static int verifyContainerLog(MiniYARNCluster yarnCluster, int containerNum,
      List<String> expectedContent, boolean count, String expectedWord) {
    File logFolder =
        new File(yarnCluster.getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                YarnConfiguration.DEFAULT_NM_LOG_DIRS));
    
    File[] listOfFiles = logFolder.listFiles();
    int currentContainerLogFileIndex = -1;
    for (int i = listOfFiles.length - 1; i >= 0; i--) {
      if (listOfFiles[i].listFiles().length == containerNum + 1) {
        currentContainerLogFileIndex = i;
        break;
      }
    }
    Assert.assertTrue(currentContainerLogFileIndex != -1);
    File[] containerFiles =
        listOfFiles[currentContainerLogFileIndex].listFiles();
    
    int numOfWords = 0;
    for (int i = 0; i < containerFiles.length; i++) {
      for (File output : containerFiles[i].listFiles()) {
        if (output.getName().trim().contains("stdout")) {
          BufferedReader br = null;
          List<String> stdOutContent = new ArrayList<String>();
          try {
            
            String sCurrentLine;
            br = new BufferedReader(new FileReader(output));
            int numOfline = 0;
            while ((sCurrentLine = br.readLine()) != null) {
              if (count) {
                if (sCurrentLine.contains(expectedWord)) {
                  numOfWords++;
                }
              } else if (output.getName().trim().equals("stdout")) {
                if (!Shell.WINDOWS) {
                  Assert.assertEquals("The current is" + sCurrentLine,
                      expectedContent.get(numOfline), sCurrentLine.trim());
                  numOfline++;
                } else {
                  stdOutContent.add(sCurrentLine.trim());
                }
              }
            }
            /* By executing bat script using cmd /c,
             * it will output all contents from bat script first
             * It is hard for us to do check line by line
             * Simply check whether output from bat file contains
             * all the expected messages
             */
            if (Shell.WINDOWS && !count
                && output.getName().trim().equals("stdout")) {
              Assert.assertTrue(stdOutContent.containsAll(expectedContent));
            }
          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            try {
              if (br != null) {
                br.close();
              }
            } catch (IOException ex) {
              ex.printStackTrace();
            }
          }
        }
      }
    }
    return numOfWords;
  }
  
  public static boolean dumpAllAggregatedContainersLogs(MiniYARNCluster yarnCluster, ApplicationId appId)
      throws Exception {
    LogsCLI logDumper = new LogsCLI();
    logDumper.setConf(yarnCluster.getConfig());
    int resultCode = logDumper.run(new String[]{"-applicationId", appId.toString()});
    return resultCode == 0;
  }
  
  public static boolean dumpAllRemoteContainersLogs(MiniYARNCluster yarnCluster, ApplicationId appId) {
    File logFolder = new File(yarnCluster
        .getNodeManager(0)
        .getConfig()
        .get(YarnConfiguration.NM_LOG_DIRS, YarnConfiguration.DEFAULT_NM_LOG_DIRS));
    File appFolder = new File(logFolder, appId.toString());
    File[] containerFolders = appFolder.listFiles();
    
    if (containerFolders == null) {
      System.out.println(appFolder + " does not have any log files.");
      return false;
    }
    
    for (File containerFolder : appFolder.listFiles()) {
      System.out.println("\n\nContainer: " + containerFolder.getName());
      System.out.println(
          "======================================================================");
      for (File logFile : containerFolder.listFiles()) {
        System.out.println("LogType:" + logFile.getName());
        BufferedReader br = null;
        try {
          String sCurrentLine;
          br = new BufferedReader(new FileReader(logFile));
          while ((sCurrentLine = br.readLine()) != null) {
            System.out.println(sCurrentLine);
          }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          try {
            if (br != null) {
              br.close();
            }
          } catch (IOException ex) {
            ex.printStackTrace();
          }
        }
        System.out.println("End of LogType:" + logFile.getName() + "\n");
      }
    }
    return true;
  }
}
