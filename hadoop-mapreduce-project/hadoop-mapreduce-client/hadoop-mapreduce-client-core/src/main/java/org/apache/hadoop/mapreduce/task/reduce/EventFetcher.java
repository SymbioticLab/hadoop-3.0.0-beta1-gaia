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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.TaskAttemptID;

class EventFetcher<K,V> extends Thread {
  private static final long SLEEP_TIME = 1000;
  private static final int MAX_RETRIES = 10;
  private static final int RETRY_PERIOD = 5000;
  private static final Log LOG = LogFactory.getLog(EventFetcher.class);

  private final TaskAttemptID reduce;
  private final TaskUmbilicalProtocol umbilical;
  private final ShuffleScheduler<K,V> scheduler;
  private int fromEventIdx = 0;
  private final int maxEventsToFetch;
  private final ExceptionReporter exceptionReporter;
  
  private volatile boolean stopped = false;

  private ConcurrentMap<TaskAttemptID, String> mapOutputFileMap;
  private LinkedBlockingQueue<Boolean> barrier;
  private JobConf job;

  public EventFetcher(ConcurrentMap<TaskAttemptID, String> mapOutputFileMap,
                      LinkedBlockingQueue<Boolean> barrier, JobConf job,
          TaskAttemptID reduce,
                      TaskUmbilicalProtocol umbilical,
                      ShuffleScheduler<K,V> scheduler,
                      ExceptionReporter reporter,
                      int maxEventsToFetch) {
    setName("EventFetcher for fetching Map Completion Events");
    setDaemon(true);    
    this.reduce = reduce;
    this.umbilical = umbilical;
    this.scheduler = scheduler;
    exceptionReporter = reporter;
    this.maxEventsToFetch = maxEventsToFetch;

    this.mapOutputFileMap = mapOutputFileMap;
    this.barrier = barrier;
    this.job = job;
  }

  @Override
  public void run() {
    int failures = 0;
    LOG.info(reduce + " Thread started: " + getName());
    
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          int numNewMaps = getMapCompletionEvents();
          failures = 0;
          if (numNewMaps > 0) {
            LOG.info(reduce + ": " + "Got " + numNewMaps + " new map-outputs");
          }
          LOG.debug("GetMapEventsThread about to sleep for " + SLEEP_TIME);
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          LOG.info("EventFetcher is interrupted.. Returning");
          return;
        } catch (IOException ie) {
          LOG.info("Exception in getting events", ie);
          // check to see whether to abort
          if (++failures >= MAX_RETRIES) {
            throw new IOException("too many failures downloading events", ie);
          }
          // sleep for a bit
          if (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(RETRY_PERIOD);
          }
        }
      }
    } catch (InterruptedException e) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
      return;
    }
  }

  public void shutDown() {
    this.stopped = true;
    interrupt();
    try {
      join(5000);
    } catch(InterruptedException ie) {
      LOG.warn("Got interrupted while joining " + getName(), ie);
    }
  }
  
  /** 
   * Queries the {@link TaskTracker} for a set of map-completion events 
   * from a given event ID.
   * @throws IOException
   */  
  protected int getMapCompletionEvents()
      throws IOException, InterruptedException {
    
    int numNewMaps = 0;
    TaskCompletionEvent events[] = null;
    String mapOutputPaths[] = null;

    ConcurrentMap<TaskAttemptID, String> tempMap = new ConcurrentHashMap<>();

    do {
      MapTaskCompletionEventsUpdate update =
          umbilical.getMapCompletionEvents(
              (org.apache.hadoop.mapred.JobID)reduce.getJobID(),
              fromEventIdx,
              maxEventsToFetch,
              (org.apache.hadoop.mapred.TaskAttemptID)reduce);
      events = update.getMapTaskCompletionEvents();
      LOG.debug("Got " + events.length + " map completion events from " +
               fromEventIdx);
      mapOutputPaths = update.getMapOutputFilePaths();

      assert !update.shouldReset() : "Unexpected legacy state";

      // Update the last seen event ID
      fromEventIdx += events.length;

      // Process the TaskCompletionEvents:
      // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
      // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop
      //    fetching from those maps.
      // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
      //    outputs at all.

      int i = 0;
      for (TaskCompletionEvent event : events) {
        scheduler.resolve(event);
        if (TaskCompletionEvent.Status.SUCCEEDED == event.getTaskStatus()) {
          ++numNewMaps;

          String str_map = event.getTaskTrackerHttp();
          // http://localhost:13562
          String host_map = str_map.substring("http://".length(), str_map.lastIndexOf(':'));

          String addr_map = new String();
          try {
            addr_map = InetAddress.getByName(host_map).getHostAddress();
          } catch (IOException e) {
            System.err.println("unkown host");
          }

          String addr_reduce = InetAddress.getLocalHost().getHostAddress();

          if (!readTopo(addr_map).equals(readTopo(addr_reduce))) {
            mapOutputFileMap.put(event.getTaskAttemptId(), mapOutputPaths[i]);
          }
          tempMap.put(event.getTaskAttemptId(), mapOutputPaths[i]);

          if (tempMap.size() == job.getNumMapTasks()) {
            barrier.put(true);
          }
        }
        i += 1;
      }
    } while (events.length == maxEventsToFetch);

    return numNewMaps;
  }

  String readTopo(String addr) {
    String rack = new String();
    try {
      Process p = new ProcessBuilder("bash", "/etc/hadoop/conf/topo.sh", addr).start();
      int exit_code = p.waitFor();
      if (exit_code != 0) {
        System.out.println("abnormal exit");
      }

      InputStream in = p.getInputStream();

      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      StringBuilder out = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        out.append(line);
      }
      reader.close();

      rack = out.toString();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("readTopo exception");
    }
    System.out.println(addr + "::" + rack);
    return rack;
  }

}
