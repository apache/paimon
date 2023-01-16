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

package org.apache.flink.table.store.file.io;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RollingFileWriter}. */
public class RollingFileWriterTest {

    private static final RowType SCHEMA =
            RowType.of(new DataType[] {new IntType()}, new String[] {"id"});

    /**
     * Set a very small target file size, so that we will roll over to a new file even if writing
     * one record.
     */
    private static final Long TARGET_FILE_SIZE = 64L;

    @TempDir java.nio.file.Path tempDir;

    private RollingFileWriter<InternalRow, DataFileMeta> rollingFileWriter;

    @BeforeEach
    public void beforeEach() {
        FileFormat fileFormat = FileFormat.fromIdentifier("avro", new Configuration());
        rollingFileWriter =
                new RollingFileWriter<>(
                        () ->
                                new RowDataFileWriter(
                                        fileFormat.createWriterFactory(SCHEMA),
                                        new DataFilePathFactory(
                                                        new Path(tempDir.toString()),
                                                        "",
                                                        0,
                                                        CoreOptions.FILE_FORMAT.defaultValue())
                                                .newPath(),
                                        SCHEMA,
                                        fileFormat.createStatsExtractor(SCHEMA).orElse(null),
                                        0L,
                                        new LongCounter(0)),
                        TARGET_FILE_SIZE);
    }

    @Test
    public void testRolling() throws IOException {
        for (int i = 0; i < 3000; i++) {
            rollingFileWriter.write(GenericRow.of(i));
            if (i < 1000) {
                assertFileNum(1);
            } else if (i < 2000) {
                assertFileNum(2);
            } else {
                assertFileNum(3);
            }
        }
    }

    private void assertFileNum(int expected) {
        File dataDir = tempDir.resolve("bucket-0").toFile();
        File[] files = dataDir.listFiles();
        assertThat(files).isNotNull().hasSize(expected);
    }
}
