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

package org.apache.paimon.disk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IOManager}. */
public class IOManagerTest {

    @TempDir Path tempDir;

    @Test
    public void channelEnumerator() throws Exception {
        File tempPath = tempDir.toFile();

        String[] tempDirs =
                new String[] {
                    new File(tempPath, "a").getAbsolutePath(),
                    new File(tempPath, "b").getAbsolutePath(),
                    new File(tempPath, "c").getAbsolutePath(),
                    new File(tempPath, "d").getAbsolutePath(),
                    new File(tempPath, "e").getAbsolutePath(),
                };

        int[] counters = new int[tempDirs.length];
        try (IOManager ioMan = IOManager.create(tempDirs)) {
            FileIOChannel.Enumerator enumerator = ioMan.createChannelEnumerator();

            for (int i = 0; i < 3 * tempDirs.length; i++) {
                FileIOChannel.ID id = enumerator.next();

                File path = id.getPathFile();

                assertThat(path.isAbsolute()).isTrue();
                assertThat(path.isDirectory()).isFalse();

                assertThat(tempPath.equals(path.getParentFile().getParentFile().getParentFile()))
                        .isTrue();

                for (int k = 0; k < tempDirs.length; k++) {
                    if (path.getParentFile().getParent().equals(tempDirs[k])) {
                        counters[k]++;
                    }
                }
            }

            for (int k = 0; k < tempDirs.length; k++) {
                assertThat(counters[k]).isEqualTo(3);
            }
        }
    }
}
