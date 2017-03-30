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

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;

public class DistributedCacheList implements Serializable {
  
  public static class Entry implements Serializable {
    public final String relativePath;
    public final URI uri;
    public final long size;
    public final long timestamp;
    
    public Entry(String relativePath, URI uri, long size, long timestamp) {
      this.relativePath = relativePath;
      this.uri = uri;
      this.size = size;
      this.timestamp = timestamp;
    }
  }
  
  private ArrayList<Entry> files = new ArrayList<>();
  
  public void add(Entry entry) {
    files.add(entry);
  }
  
  public Entry get(int index) {
    return files.get(index);
  }
  
  public int size() {
    return files.size();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (Entry e : files) {
      sb.append("(" + e.relativePath + ", " + e.uri + ", " + e.size + ", " + e.timestamp + "),\n");
    }
    if (files.size() > 0) {
      sb.setLength(sb.length() - 2);
    }
    sb.append("]");
    return sb.toString();
  }
}
