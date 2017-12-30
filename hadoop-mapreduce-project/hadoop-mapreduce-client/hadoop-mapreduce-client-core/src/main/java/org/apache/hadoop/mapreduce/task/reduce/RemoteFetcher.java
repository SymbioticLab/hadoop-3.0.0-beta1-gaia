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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SpillRecord;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.CryptoUtils;

import org.apache.hadoop.fs.Path;

/**
 * LocalFetcher is used by LocalJobRunner to perform a local filesystem
 * fetch.
 */
class RemoteFetcher<K,V> extends Fetcher<K, V> {

  private static final Log LOG = LogFactory.getLog(LocalFetcher.class);

  private static final MapHost LOCALHOST = new MapHost("local", "local");

  private JobConf job;
  // private Map<TaskAttemptID, MapOutputFile> localMapFiles;
  private TaskAttemptID mapAttemptID;
  private String mapOutputFile;
  private TaskAttemptID reduceAttemptID;

  public RemoteFetcher(TaskAttemptID mapAttempID, String mapOutputFile,
                      JobConf job, TaskAttemptID reduceId,
                      ShuffleSchedulerImpl<K, V> scheduler,
                      MergeManager<K,V> merger,
                      Reporter reporter, ShuffleClientMetrics metrics,
                      ExceptionReporter exceptionReporter,
                      SecretKey shuffleKey,
                      Map<TaskAttemptID, MapOutputFile> localMapFiles) {
    super(job, reduceId, scheduler, merger, reporter, metrics,
        exceptionReporter, shuffleKey);

    this.job = job;
    // this.localMapFiles = localMapFiles;

    setName("remotefetcher#" + id);
    setDaemon(true);

    this.mapAttemptID = mapAttempID;
    this.mapOutputFile = mapOutputFile;
    this.reduceAttemptID = reduceId;
  }

  public void run() {
    // Create a worklist of task attempts to work over.
    Set<TaskAttemptID> maps = new HashSet<TaskAttemptID>();
    maps.add(mapAttemptID);

    while (maps.size() > 0) {
      try {
        // If merge is on, block
        merger.waitForResource();
        metrics.threadBusy();

        // Copy as much as is possible.
        doCopy(maps);
        metrics.threadFree();
      } catch (InterruptedException ie) {
      } catch (Throwable t) {
        exceptionReporter.reportException(t);
      }
    }
  }

  /**
   * The crux of the matter...
   */
  private void doCopy(Set<TaskAttemptID> maps) throws IOException {
    Iterator<TaskAttemptID> iter = maps.iterator();
    while (iter.hasNext()) {
      TaskAttemptID map = iter.next();
      LOG.debug("LocalFetcher " + id + " going to fetch: " + map);
      if (copyMapOutput(map)) {
        // Successful copy. Remove this from our worklist.
        iter.remove();
      } else {
        // We got back a WAIT command; go back to the outer loop
        // and block for InMemoryMerge.
        break;
      }
    }
  }

  /**
   * Retrieve the map output of a single map task
   * and send it to the merger.
   */
  private boolean copyMapOutput(TaskAttemptID mapTaskId) throws IOException {
    // Figure out where the map task stored its output.
    assert (mapOutputFile != null);

    // Path mapOutputFileName = new Path(mapOutputFile);
    // Path indexFileName = mapOutputFileName.suffix(".index");

    int pos_dot = mapOutputFile.lastIndexOf('.');
    if (pos_dot == -1) {
      LOG.info("filename wrong ??? " + mapOutputFile);
    }

    String str_file = mapOutputFile.substring(0, pos_dot);
    String str_dot_out = mapOutputFile.substring(pos_dot);

    Path newMapOutputFileName = new Path(str_file + "_" +
            reduceAttemptID.toString() + str_dot_out);
    LOG.info("# newmapoutputfilename #" + str_file + "_" +
            reduceAttemptID.toString() + str_dot_out);

    // Read its index to determine the location of our split
    // and its size.

    RandomAccessFile raf = new RandomAccessFile(newMapOutputFileName.toString(), "r");
    long compressedLength = raf.length();

    long decompressedLength = compressedLength - 4;
    LOG.info("new file length: " + compressedLength);

    compressedLength -= CryptoUtils.cryptoPadding(job);
    decompressedLength -= CryptoUtils.cryptoPadding(job);

    // Get the location for the map output - either in-memory or on-disk
    MapOutput<K, V> mapOutput = merger.reserve(mapTaskId, decompressedLength,
        id);

    // Check if we can shuffle *now* ...
    if (mapOutput == null) {
      LOG.info("fetcher#" + id + " - MergeManager returned Status.WAIT ...");
      return false;
    }

    // Go!
    LOG.info("remotefetcher#" + id + " about to shuffle output of map " +
             mapOutput.getMapId() + " decomp: " +
             decompressedLength + " len: " + compressedLength + " to " +
             mapOutput.getDescription());

    // now read the file, seek to the appropriate section, and send it.
    FileSystem localFs = FileSystem.getLocal(job).getRaw();

    LOG.info("***localFS Uri*** " + localFs.getUri());

    FSDataInputStream inStream = localFs.open(newMapOutputFileName);

    try {
      inStream = CryptoUtils.wrapIfNecessary(job, inStream);
      inStream.seek(0 + CryptoUtils.cryptoPadding(job));
      mapOutput.shuffle(LOCALHOST, inStream, compressedLength,
          decompressedLength, metrics, reporter);
    } finally {
      IOUtils.cleanup(LOG, inStream);
    }

    scheduler.copySucceeded(mapTaskId, LOCALHOST, compressedLength, 0, 0,
        mapOutput);
    return true; // successful fetch.
  }
}

