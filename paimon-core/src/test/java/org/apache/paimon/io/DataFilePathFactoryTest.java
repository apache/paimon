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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFilePathFactory}. */
public class DataFilePathFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testNoPartition() {
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-123"),
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        String uuid = pathFactory.uuid();

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newPath())
                    .isEqualTo(
                            new Path(
                                    tempDir.toString()
                                            + "/bucket-123/data-"
                                            + uuid
                                            + "-"
                                            + i
                                            + "."
                                            + CoreOptions.FILE_FORMAT.defaultValue()));
        }
        assertThat(pathFactory.toPath("my-data-file-name", null))
                .isEqualTo(new Path(tempDir.toString() + "/bucket-123/my-data-file-name"));
    }

    @Test
    public void testWithPartition() {
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/dt=20211224/bucket-123"),
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        String uuid = pathFactory.uuid();

        for (int i = 0; i < 20; i++) {
            assertThat(pathFactory.newPath())
                    .isEqualTo(
                            new Path(
                                    tempDir.toString()
                                            + "/dt=20211224/bucket-123/data-"
                                            + uuid
                                            + "-"
                                            + i
                                            + "."
                                            + CoreOptions.FILE_FORMAT.defaultValue()));
        }
        assertThat(pathFactory.toPath("my-data-file-name", null))
                .isEqualTo(
                        new Path(tempDir.toString() + "/dt=20211224/bucket-123/my-data-file-name"));
    }
}
