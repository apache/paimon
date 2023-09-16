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

package org.apache.paimon.format.avro;

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.IntStream;

/** {@link TableStatsExtractor} for avro files. */
public class AvroTableStatsExtractor implements TableStatsExtractor {

    private final RowType rowType;
    private final FieldStatsCollector.Factory[] statsCollectors;

    public AvroTableStatsExtractor(RowType rowType, FieldStatsCollector.Factory[] statsCollectors) {
        this.rowType = rowType;
        this.statsCollectors = statsCollectors;
        Preconditions.checkArgument(
                rowType.getFieldCount() == statsCollectors.length,
                "The stats collector is not aligned to write schema.");
    }

    @Override
    public FieldStats[] extract(FileIO fileIO, Path path, byte[] plaintextKey, byte[] aadPrefix)
            throws IOException {
        return extractWithFileInfo(fileIO, path, plaintextKey, aadPrefix).getLeft();
    }

    @Override
    public Pair<FieldStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, byte[] plaintextKey, byte[] aadPrefix) throws IOException {

        SeekableInputStream fileInputStream = fileIO.newInputStream(path);
        long rowCount = getRowCount(fileInputStream);

        return Pair.of(
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i -> {
                                    // In avro format, minValue, maxValue, and nullCount are not
                                    // counted. So fill it with null.
                                    return new FieldStats(null, null, null);
                                })
                        .toArray(FieldStats[]::new),
                new FileInfo(rowCount));
    }

    private long getRowCount(InputStream inStream) throws java.io.IOException {
        // an Avro file's layout looks like this:
        //   header|block|block|...
        // the header contains:
        //   magic|string-map|sync
        // each block consists of:
        //   row-count|compressed-size-in-bytes|block-bytes|sync
        // So we accumulate row-count in each block.

        long count = 0L;
        try (DataFileStream<Object> streamReader =
                new DataFileStream<>(inStream, new GenericDatumReader<>())) {
            while (streamReader.hasNext()) {
                count = count + streamReader.getBlockCount();
                streamReader.nextBlock();
            }
        }
        return count;
    }
}
