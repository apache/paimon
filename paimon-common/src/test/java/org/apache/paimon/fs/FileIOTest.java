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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link FileIO}. */
public class FileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRequireOptions() throws IOException {
        Options options = new Options();

        try {
            FileIO.get(
                    new Path("require-options://" + tempDir.toString()),
                    CatalogContext.create(options));
            Assertions.fail();
        } catch (UnsupportedSchemeException e) {
            assertThat(e.getSuppressed()[0])
                    .hasMessageContaining("Missing required options are:\n\nRequire1\nreQuire2");
        }

        options.set("Re-quire1", "dummy");
        options.set("reQuire2", "dummy");
        FileIO fileIO =
                FileIO.get(
                        new Path("require-options://" + tempDir.toString()),
                        CatalogContext.create(options));
        assertThat(fileIO).isInstanceOf(RequireOptionsFileIOLoader.MyFileIO.class);
    }

    @Test
    public void testDiscoverLoaders() throws InterruptedException {
        List<Map<String, FileIOLoader>> synchronizedList =
                Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Exception> exception = new AtomicReference<>();
        final int max = 10;

        Runnable runnable =
                () -> {
                    for (int i = 0; i < max; i++) {
                        try {
                            synchronizedList.add(FileIO.discoverLoaders());
                            Thread.sleep(100);
                        } catch (Exception e) {
                            exception.set(e);
                            return;
                        }
                    }
                };

        Thread thread1 = new Thread(runnable);
        Thread thread2 = new Thread(runnable);

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        assertThat(exception.get()).isNull();
        assertThat(synchronizedList).isNotEmpty().containsOnly(FileIO.discoverLoaders());
    }

    public static void testOverwriteFileUtf8(Path file, FileIO fileIO) throws InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        final int max = 10;

        Thread writeThread =
                new Thread(
                        () -> {
                            for (int i = 0; i <= max; i++) {
                                try {
                                    fileIO.overwriteFileUtf8(file, "" + i);
                                    Thread.sleep(100);
                                } catch (Exception e) {
                                    exception.set(e);
                                    return;
                                }
                            }
                        });

        Thread readThread =
                new Thread(
                        () -> {
                            while (true) {
                                try {
                                    Optional<String> ret = fileIO.readOverwrittenFileUtf8(file);
                                    if (!ret.isPresent()) {
                                        continue;
                                    }

                                    int value = Integer.parseInt(ret.get());
                                    if (value == max) {
                                        return;
                                    }
                                } catch (Exception e) {
                                    exception.set(e);
                                    return;
                                }
                            }
                        });

        writeThread.start();
        readThread.start();

        writeThread.join();
        readThread.join();

        assertThat(exception.get()).isNull();
    }
}
