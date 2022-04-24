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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link FileUtils}. */
public class FileUtilsTest {

    @TempDir private java.nio.file.Path tempDir;
    private Path snapshotDir;
    private File foo;
    private File tempBar;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    public void beforeEach() throws IOException {
        Path root = new Path(tempDir.toString());
        snapshotDir = new Path(root + "/snapshot");
        root.getFileSystem().mkdirs(snapshotDir);
        foo = new File(snapshotDir.getPath() + "/foo");
        foo.createNewFile();
        tempBar = new File(snapshotDir.getPath() + "/" + "bar.temp");
        tempBar.createNewFile();
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Test
    public void testFindByListFilesWithRetry() throws IOException {
        scheduler.schedule(deleteTask(), 3, TimeUnit.SECONDS);
        scheduler.shutdown();
        Path fooPath = Path.fromLocalFile(foo);
        FileStatus[] statuses =
                FileUtils.safelyListFileStatus(
                        UnsafeLocalFileSystem.getUnsafePath(snapshotDir.getPath()));
        assertThat(statuses).hasSize(1);
        assertThat(statuses[0].getPath()).isEqualTo(fooPath);
    }

    private Runnable deleteTask() {
        return new Runnable() {
            int ctr = 1;

            @Override
            public void run() {
                while (ctr <= 2) {
                    synchronized (UnsafeLocalFileSystem.SHARED_LOCK) {
                        if (tempBar.exists()) {
                            tempBar.delete();
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
        };
    }
}
