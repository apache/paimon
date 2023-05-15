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

package org.apache.paimon.flink.util;

import org.apache.paimon.utils.FileIOUtils;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/** Similar to Flink's AbstractTestBase but using Junit5. */
public class AbstractTestBase {

    private static final int DEFAULT_PARALLELISM = 8;

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    @TempDir protected static Path temporaryFolder;

    @AfterEach
    public final void cleanupRunningJobs() throws Exception {
        ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient();
        for (JobStatusMessage path : clusterClient.listJobs().get()) {
            if (!path.getJobState().isTerminalState()) {
                try {
                    clusterClient.cancel(path.getJobId()).get();
                } catch (Exception ignored) {
                    // ignore exceptions when cancelling dangling jobs
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Temporary File Utilities
    // ----------------------------------------------------------------------------------------------------------------

    protected String getTempDirPath() {
        return getTempDirPath("");
    }

    protected String getTempDirPath(String dirName) {
        return createAndRegisterTempFile(dirName).toString();
    }

    protected String getTempFilePath(String fileName) {
        return createAndRegisterTempFile(fileName).toString();
    }

    protected String createTempFile(String fileName, String contents) throws IOException {
        File f = createAndRegisterTempFile(fileName);
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        FileIOUtils.writeFileUtf8(f, contents);
        return f.toString();
    }

    /** Create a subfolder to avoid returning the same folder when passing same file name. */
    protected File createAndRegisterTempFile(String fileName) {
        return new File(
                temporaryFolder.toFile(), String.format("%s/%s", UUID.randomUUID(), fileName));
    }
}
