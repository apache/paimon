/*
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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** PWC HDFS state Manage using OperatorID . path: baseDir/operatorId/checkpoint-{ckId}.state */
public class PwcStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(PwcStateManager.class);
    private static final String FILE_SUFFIX = ".state";

    private final FileSystem fs;
    private final Path basePath;
    private final PwcStateSerializer serializer;
    private final int retryTimes;

    public PwcStateManager(
            String baseDir, OperatorID operatorId, PwcStateSerializer serializer, int retryTimes)
            throws IOException {

        this.serializer = serializer;
        this.retryTimes = retryTimes;
        this.basePath = new Path(baseDir, operatorId.toHexString());

        Configuration hadoopConf = new Configuration();
        this.fs = FileSystem.get(basePath.toUri(), hadoopConf);

        if (!fs.exists(basePath)) {
            fs.mkdirs(basePath);
        }

        LOG.info("PwcStateManager initialized: basePath={}", basePath);
    }

    public void writeState(PwcState state) throws IOException {
        long checkpointId = state.checkpointId;
        Path finalPath = getPath(checkpointId);
        Path tmpPath = new Path(finalPath.getParent(), finalPath.getName() + ".tmp");

        IOException lastException = null;

        for (int i = 0; i < retryTimes; i++) {
            try {
                byte[] bytes = serializer.serialize(state);

                try (FSDataOutputStream out = fs.create(tmpPath, true)) {
                    out.write(bytes);
                    out.hsync();
                }

                if (fs.rename(tmpPath, finalPath)) {
                    LOG.debug("Wrote PWC state for checkpoint-{} to {}", checkpointId, finalPath);
                    return;
                }

                throw new IOException("Rename failed: " + tmpPath + " -> " + finalPath);

            } catch (IOException e) {
                lastException = e;
                LOG.warn(
                        "Write state failed (attempt {}/{}): {}",
                        i + 1,
                        retryTimes,
                        e.getMessage());
                tryDelete(tmpPath);
            }
        }

        throw new IOException("Write state failed after " + retryTimes + " retries", lastException);
    }

    public PwcState readState(long checkpointId) throws IOException {
        Path path = getPath(checkpointId);
        if (!fs.exists(path)) {
            return null;
        }

        try (FSDataInputStream in = fs.open(path)) {
            byte[] bytes = new byte[(int) fs.getFileStatus(path).getLen()];
            in.readFully(bytes);
            return serializer.deserialize(bytes);
        }
    }

    public void deleteState(long checkpointId) {
        tryDelete(getPath(checkpointId));
    }

    public List<Long> listCheckpointIds() throws IOException {
        List<Long> result = new ArrayList<>();

        if (!fs.exists(basePath)) {
            return result;
        }

        FileStatus[] files = fs.listStatus(basePath, path -> path.getName().endsWith(FILE_SUFFIX));

        for (FileStatus file : files) {
            String name = file.getPath().getName();
            try {
                String idStr =
                        name.substring(
                                "checkpoint-".length(), name.length() - FILE_SUFFIX.length());
                result.add(Long.parseLong(idStr));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid checkpoint file name: {}", name);
            }
        }

        return result;
    }

    public void cleanupAll() throws IOException {
        if (!fs.exists(basePath)) {
            return;
        }

        FileStatus[] files = fs.listStatus(basePath);
        for (FileStatus file : files) {
            fs.delete(file.getPath(), true);
        }

        LOG.info("Cleaned up all PWC states in {}", basePath);
    }

    private Path getPath(long checkpointId) {
        return new Path(basePath, "checkpoint-" + checkpointId + FILE_SUFFIX);
    }

    private void tryDelete(Path path) {
        try {
            if (fs.exists(path)) {
                fs.delete(path, false);
                LOG.debug("Deleted PWC state: {}", path);
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete PWC state: {}", path, e);
        }
    }
}
