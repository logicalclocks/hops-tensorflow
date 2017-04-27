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

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class TensorBoardServer {
  
  /**
   * Spawns a HTTP server that provides TensorBoard connection information.
   *
   * @return port used by server
   */
  public static int spawn(final ApplicationMaster am) throws IOException {
    ServerSocket s = new ServerSocket(0);
    int port = s.getLocalPort();
    s.close();
    
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/tensorboard", new HttpHandler() {
      @Override
      public void handle(HttpExchange he) throws IOException {
        String response = new Gson().toJson(am.getTensorBoardEndpoint()); // List<String>
        he.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        he.sendResponseHeaders(200, response.length());
        he.getResponseBody().write(response.getBytes());
      }
    });
    server.start();
    
    return port;
  }
  
  private TensorBoardServer() {}
  
}
