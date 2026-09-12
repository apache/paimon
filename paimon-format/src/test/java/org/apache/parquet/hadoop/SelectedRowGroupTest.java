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

package org.apache.parquet.hadoop;

import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetInputStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Row IDs must reject unrelated groups before dictionary I/O, without weakening predicates. */
class SelectedRowGroupTest {

    @Test
    void testOnlySelectedDictionaryIsReadAndPredicateStillFilters() throws Exception {
        assertSelection(5, false, "x", 1, Collections.singletonList(0));
        assertSelection(15, false, "x", 1, Collections.singletonList(1));
        assertSelection(5, false, "y", 0, Collections.singletonList(0));
        assertSelection(-1, false, "x", 0, Collections.emptyList());
    }

    @Test
    void testMissingOffsetsKeepExistingFallback() throws Exception {
        assertSelection(5, true, "x", 2, Collections.emptyList());
    }

    private void assertSelection(
            int selected,
            boolean missingOffset,
            String value,
            int groupCount,
            List<Integer> expectedDictionaries)
            throws Exception {
        MessageType schema =
                MessageTypeParser.parseMessageType("message m { required binary tag (UTF8); }");
        List<BlockMetaData> blocks = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            BlockMetaData block = new BlockMetaData();
            block.setRowCount(10);
            block.setRowIndexOffset(missingOffset && i == 1 ? -1 : i * 10);
            ColumnChunkMetaData column =
                    ColumnChunkMetaData.get(
                            ColumnPath.get("tag"),
                            schema.getType("tag").asPrimitiveType(),
                            CompressionCodecName.UNCOMPRESSED,
                            new EncodingStats.Builder()
                                    .addDictEncoding(Encoding.PLAIN)
                                    .addDataEncoding(Encoding.RLE_DICTIONARY)
                                    .build(),
                            new HashSet<>(Arrays.asList(Encoding.PLAIN, Encoding.RLE_DICTIONARY)),
                            new BinaryStatistics(),
                            20 + i * 100,
                            4 + i * 100,
                            10,
                            50,
                            50);
            column.setRowGroupOrdinal(i);
            block.addColumn(column);
            blocks.add(block);
        }
        ParquetMetadata footer =
                new ParquetMetadata(
                        new FileMetaData(schema, Collections.emptyMap(), "test"), blocks);
        RoaringBitmap32 selection = new RoaringBitmap32();
        if (selected >= 0) {
            selection.add(selected);
        }
        ParquetReadOptions options =
                ParquetReadOptions.builder()
                        .withRecordFilter(
                                FilterCompat.get(
                                        FilterApi.eq(
                                                FilterApi.binaryColumn("tag"),
                                                Binary.fromString(value))))
                        .useStatsFilter(false)
                        .useDictionaryFilter(true)
                        .useBloomFilter(false)
                        .build();
        List<Integer> dictionaries = new ArrayList<>();
        ParquetInputFile file =
                ParquetInputFile.fromPath(LocalFileIO.create(), new Path("/unused.parquet"), 200);
        ParquetInputStream input =
                new ParquetInputStream(
                        SeekableInputStream.wrap(new ByteArrayInputStream(new byte[0])));
        try (ParquetFileReader reader =
                new ParquetFileReader(file, footer, options, input, selection) {
                    @Override
                    DictionaryPage readDictionary(ColumnChunkMetaData column) {
                        dictionaries.add(column.getRowGroupOrdinal());
                        return new DictionaryPage(
                                BytesInput.from(new byte[] {1, 0, 0, 0, 'x'}), 1, Encoding.PLAIN);
                    }
                }) {
            assertThat(reader.getRowGroups()).hasSize(groupCount);
            assertThat(dictionaries).isEqualTo(expectedDictionaries);
        }
    }
}
