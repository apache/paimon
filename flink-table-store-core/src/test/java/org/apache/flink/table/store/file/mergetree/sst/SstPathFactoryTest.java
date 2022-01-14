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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SstPathFactory}. */
public class SstPathFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testNoPartition() {
        SstPathFactory pathFactory = new SstPathFactory(new Path(tempDir.toString()), "", 123);
        String uuid = pathFactory.uuid();

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newPath())
                    .isEqualTo(new Path(tempDir.toString() + "/bucket-123/sst-" + uuid + "-" + i));
        }
        assertThat(pathFactory.toPath("my-sst-file-name"))
                .isEqualTo(new Path(tempDir.toString() + "/bucket-123/my-sst-file-name"));
    }

    @Test
    public void testWithPartition() {
        SstPathFactory pathFactory =
                new SstPathFactory(new Path(tempDir.toString()), "dt=20211224", 123);
        String uuid = pathFactory.uuid();

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newPath())
                    .isEqualTo(
                            new Path(
                                    tempDir.toString()
                                            + "/dt=20211224/bucket-123/sst-"
                                            + uuid
                                            + "-"
                                            + i));
        }
        assertThat(pathFactory.toPath("my-sst-file-name"))
                .isEqualTo(
                        new Path(tempDir.toString() + "/dt=20211224/bucket-123/my-sst-file-name"));
    }
}
