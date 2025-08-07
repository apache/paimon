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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FormatDataSplit}. */
public class FormatDataSplitTest {

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        // Create a mock FileIO (we don't need actual file operations for this test)
        FileIO fileIO =
                new FileIO() {
                    @Override
                    public boolean exists(Path path) throws IOException {
                        return false;
                    }

                    @Override
                    public boolean delete(Path path, boolean recursive) throws IOException {
                        return true;
                    }

                    @Override
                    public FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
                        return new FileStatus[0];
                    }

                    @Override
                    public boolean mkdirs(Path path) throws IOException {
                        return true;
                    }

                    @Override
                    public boolean rename(Path src, Path dst) throws IOException {
                        return true;
                    }

                    @Override
                    public boolean isObjectStore() {
                        return false;
                    }

                    @Override
                    public void configure(CatalogContext context) {}

                    @Override
                    public SeekableInputStream newInputStream(Path path) throws IOException {
                        return null;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        return null;
                    }

                    @Override
                    public FileStatus getFileStatus(Path path) throws IOException {
                        return null;
                    }

                    @Override
                    public FileStatus[] listStatus(Path path) throws IOException {
                        return new FileStatus[0];
                    }
                };

        // Create test data
        Path filePath = new Path("/test/path/file.parquet");
        RowType rowType = RowType.builder().field("id", new IntType()).build();
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("partition1", "value1");
        long modificationTime = System.currentTimeMillis();

        // Create a predicate for testing
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.equal(0, 5);

        // Create FormatDataSplit
        FormatDataSplit split =
                new FormatDataSplit(
                        fileIO,
                        filePath,
                        100L, // offset
                        1024L, // length
                        rowType,
                        partitionSpec,
                        modificationTime,
                        predicate,
                        new int[] {0} // projection
                        );

        // Test Java serialization
        byte[] serialized = InstantiationUtil.serializeObject(split);
        FormatDataSplit deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify the deserialized object
        assertThat(deserialized.filePath()).isEqualTo(filePath);
        assertThat(deserialized.offset()).isEqualTo(100L);
        assertThat(deserialized.length()).isEqualTo(1024L);
        assertThat(deserialized.rowType()).isEqualTo(rowType);
        assertThat(deserialized.predicate()).isEqualTo(predicate);
        assertThat(deserialized.projection()).containsExactly(0);
    }
}
