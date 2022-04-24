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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.store.file.utils.SnapshotFinder.SNAPSHOT_PREFIX;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link SnapshotFinder}. */
public class SnapshotFinderTest {

    @TempDir private java.nio.file.Path tempDir;
    private Path snapshotDir;
    private File tempLatest;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void beforeEach() throws IOException {
        Path root = new Path(tempDir.toString());
        snapshotDir = new Path(root + "/snapshot");
        root.getFileSystem().mkdirs(snapshotDir);
        new File(snapshotDir.getPath() + "/" + SNAPSHOT_PREFIX + "0").createNewFile();
        tempLatest = new File(snapshotDir.getPath() + "/" + "LATEST.temp");
        tempLatest.createNewFile();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Test
    public void testFindByListFilesWithRetry() throws IOException {
        // retry once
        int maxRetry = 1;
        scheduler.schedule(deleteTask(maxRetry), 2, TimeUnit.SECONDS);
        scheduler.shutdown();
        assertThat(
                        SnapshotFinder.findByListFiles(
                                UnsafeLocalFileSystem.getUnsafePath(snapshotDir.getPath()),
                                Long::max,
                                maxRetry))
                .isEqualTo(0);
    }

    @Test
    public void testFindByListFilesWithoutRetry() {
        // test no retry
        int maxRetry = 0;
        scheduler.schedule(deleteTask(maxRetry), 2, TimeUnit.SECONDS);
        scheduler.shutdown();
        assertThatThrownBy(
                        () ->
                                SnapshotFinder.findByListFiles(
                                        UnsafeLocalFileSystem.getUnsafePath(snapshotDir.getPath()),
                                        Long::max,
                                        maxRetry))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "The return value is null of the listStatus for the snapshot directory.");
    }

    private Runnable deleteTask(int maxRetry) {
        return new Runnable() {
            int ctr = 0;

            @Override
            public void run() {
                {
                    while (ctr <= maxRetry) {
                        synchronized (UnsafeLocalFileSystem.SHARED_LOCK) {
                            if (tempLatest.exists()) {
                                tempLatest.delete();
                                UnsafeLocalFileSystem.SHARED_LOCK.notify();
                                try {
                                    UnsafeLocalFileSystem.SHARED_LOCK.wait();
                                } catch (InterruptedException ignored) {
                                }
                            } else {
                                UnsafeLocalFileSystem.SHARED_LOCK.notify();
                            }
                        }
                        ctr++;
                    }
                }
            }
        };
    }
}
