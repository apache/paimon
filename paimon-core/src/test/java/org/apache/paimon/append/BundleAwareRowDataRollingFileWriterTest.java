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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BundleAwareRowDataRollingFileWriter}. */
public class BundleAwareRowDataRollingFileWriterTest {

    private static final RowType SCHEMA =
            RowType.of(new DataType[] {new IntType()}, new String[] {"id"});

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWriteBundlePreservesSequenceSideEffects() throws IOException {
        FileFormat fileFormat = FileFormat.fromIdentifier("parquet", new Options());
        SimpleColStatsCollector.Factory[] statsCollectors =
                SimpleColStatsCollector.createFullStatsFactories(SCHEMA.getFieldCount());
        LongCounter seqNumCounter = new LongCounter();
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-0"),
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);

        BundleAwareRowDataRollingFileWriter writer =
                new BundleAwareRowDataRollingFileWriter(
                        () ->
                                new BundleAwareRowDataFileWriter(
                                        LocalFileIO.create(),
                                        RollingFileWriter.createFileWriterContext(
                                                fileFormat,
                                                SCHEMA,
                                                statsCollectors,
                                                CoreOptions.FILE_COMPRESSION.defaultValue()),
                                        pathFactory.newPath(),
                                        SCHEMA,
                                        0L,
                                        () -> seqNumCounter,
                                        new FileIndexOptions(),
                                        FileSource.APPEND,
                                        false,
                                        false,
                                        false,
                                        null),
                        BundleAwareRowDataRollingFileWriter.supportsBundlePassThrough(
                                fileFormat, SCHEMA, statsCollectors),
                        1024L * 1024L);

        writer.writeBundle(
                new ReplayableTestBundleRecords(
                        Arrays.<InternalRow>asList(
                                GenericRow.of(1), GenericRow.of(2), GenericRow.of(3))));
        writer.close();

        DataFileMeta file = writer.result().get(0);
        assertThat(file.rowCount()).isEqualTo(3);
        assertThat(file.minSequenceNumber()).isEqualTo(0);
        assertThat(file.maxSequenceNumber()).isEqualTo(2);
        assertThat(seqNumCounter.getValue()).isEqualTo(3);
    }

    private static class ReplayableTestBundleRecords implements ReplayableBundleRecords {

        private final List<InternalRow> rows;

        private ReplayableTestBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }
}
