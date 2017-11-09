/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.sun.jersey.core.impl.provider.header.WriterUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A class that represents the communication between the tasktracker and child
 * tasks w.r.t the map task completion events. It also indicates whether the
 * child task should reset its events index.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapTaskCompletionEventsUpdate implements Writable {
  TaskCompletionEvent[] events;
  String[] mapOutputFilePaths;
  boolean reset;

  public MapTaskCompletionEventsUpdate() { }

  public MapTaskCompletionEventsUpdate(TaskCompletionEvent[] events,
      String[] mapOutputFilePaths,
      boolean reset) {
    this.events = events;
    this.mapOutputFilePaths = mapOutputFilePaths;
    this.reset = reset;
  }

  public boolean shouldReset() {
    return reset;
  }

  public TaskCompletionEvent[] getMapTaskCompletionEvents() {
    return events;
  }

  public String[] getMapOutputFilePaths() {
    return mapOutputFilePaths;
  }

  public void write(DataOutput out) throws IOException {
    out.writeBoolean(reset);
    out.writeInt(events.length);
    for (TaskCompletionEvent event : events) {
      event.write(out);
    }

    out.write(mapOutputFilePaths.length);
    for (String mapOutputFilePath: mapOutputFilePaths) {
      WritableUtils.writeString(out, mapOutputFilePath);
    }
  }

  public void readFields(DataInput in) throws IOException {
    reset = in.readBoolean();
    events = new TaskCompletionEvent[in.readInt()];
    for (int i = 0; i < events.length; ++i) {
      events[i] = new TaskCompletionEvent();
      events[i].readFields(in);
    }

    mapOutputFilePaths = new String[in.readInt()];
    for (int i = 0; i < mapOutputFilePaths.length; i += 1) {
      mapOutputFilePaths[i] = WritableUtils.readString(in);
    }
  }
}
