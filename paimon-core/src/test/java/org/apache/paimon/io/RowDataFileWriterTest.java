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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.BlobRefType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataFileWriter}. */
public class RowDataFileWriterTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new DataType[] {new IntType(), new BlobRefType()}, new String[] {"id", "ref"});

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWriteBlobRefFile() throws Exception {
        FileFormat fileFormat = FileFormat.fromIdentifier("parquet", new Options());
        Path dataPath = new Path(tempDir.toUri().toString(), "data.parquet");
        BlobReference reference = new BlobReference("default.upstream", 7, 11L);

        RowDataFileWriter writer =
                new RowDataFileWriter(
                        LocalFileIO.create(),
                        RollingFileWriter.createFileWriterContext(
                                fileFormat,
                                SCHEMA,
                                new SimpleColStatsCollector.Factory[] {
                                    NoneSimpleColStatsCollector::new,
                                    NoneSimpleColStatsCollector::new
                                },
                                CoreOptions.FILE_COMPRESSION.defaultValue()),
                        dataPath,
                        SCHEMA,
                        0L,
                        () -> new LongCounter(0),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        false,
                        SCHEMA.getFieldNames());

        writer.write(GenericRow.of(1, Blob.fromReference(reference)));
        writer.close();

        DataFileMeta meta = writer.result();

        // No .blobref extra files should be produced
        assertThat(meta.extraFiles().stream().noneMatch(f -> f.endsWith(".blobref"))).isTrue();
        assertThat(meta.rowCount()).isEqualTo(1);
    }
}
