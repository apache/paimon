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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.append.BucketedAppendCompactManager;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileReadWriteTest;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;

/** test file format suffix. */
public class FileFormatSuffixTest extends KeyValueFileReadWriteTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new DataType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});

    @Test
    public void testFileSuffix(@TempDir java.nio.file.Path tempDir) throws Exception {
        String format = "avro";
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), format);
        Path path = writerFactory.pathFactory(0).newPath();
        assertThat(path.toString().endsWith(format)).isTrue();

        DataFilePathFactory dataFilePathFactory =
                new DataFilePathFactory(new Path(tempDir + "/dt=1/bucket-1"), format);
        FileFormat fileFormat = FileFormat.fromIdentifier(format, new Options());
        LinkedList<DataFileMeta> toCompact = new LinkedList<>();
        CoreOptions options = new CoreOptions(new HashMap<>());
        AppendOnlyWriter appendOnlyWriter =
                new AppendOnlyWriter(
                        LocalFileIO.create(),
                        IOManager.create(tempDir.toString()),
                        0,
                        fileFormat,
                        10,
                        SCHEMA,
                        0,
                        new BucketedAppendCompactManager(
                                null, toCompact, null, 4, 10, 10, null, null), // not used
                        null,
                        false,
                        dataFilePathFactory,
                        null,
                        false,
                        false,
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        CompressOptions.defaultOptions(),
                        StatsCollectorFactories.createStatsFactories(
                                options, SCHEMA.getFieldNames()),
                        MemorySize.MAX_VALUE,
                        new FileIndexOptions(),
                        true);
        appendOnlyWriter.setMemoryPool(
                new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        appendOnlyWriter.write(
                GenericRow.of(1, BinaryString.fromString("aaa"), BinaryString.fromString("1")));
        CommitIncrement increment = appendOnlyWriter.prepareCommit(true);
        appendOnlyWriter.close();

        DataFileMeta meta = increment.newFilesIncrement().newFiles().get(0);
        assertThat(meta.fileName().endsWith(format)).isTrue();
    }
}
