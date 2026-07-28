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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests building one file-aligned primary-key full-text archive. */
class PkFullTextIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testWritesEveryPhysicalOrdinalAndDataLevelMetadata() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkFullTextDataFileReader reader = mock(PkFullTextDataFileReader.class);
        when(reader.rowCount()).thenReturn(3L);
        when(reader.readNextText())
                .thenReturn(
                        BinaryString.fromString("first"), null, BinaryString.fromString("third"));
        RecordingIndexer indexer = new RecordingIndexer();

        IndexFileMeta archive =
                new PkFullTextIndexFile(fileIO, pathFactory())
                        .build(
                                dataFile("data-1", 3),
                                reader,
                                new DataField(7, "content", DataTypes.STRING()),
                                indexer);

        assertThat(indexer.rowIds).containsExactly(0L, 1L, 2L);
        assertThat(indexer.values)
                .containsExactly(
                        BinaryString.fromString("first"), null, BinaryString.fromString("third"));
        assertThat(archive.indexType()).isEqualTo("full-text");
        assertThat(archive.rowCount()).isEqualTo(3);
        assertThat(archive.globalIndexMeta().rowRangeStart()).isZero();
        assertThat(archive.globalIndexMeta().rowRangeEnd()).isEqualTo(2);
        assertThat(archive.globalIndexMeta().indexFieldId()).isEqualTo(7);
        DataInputDeserializer sourceInput =
                new DataInputDeserializer(archive.globalIndexMeta().sourceMeta());
        assertThat(sourceInput.readInt()).isEqualTo(1);
        PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(archive);
        assertThat(sourceMeta.dataLevel()).isEqualTo(3);
        assertThat(sourceMeta.sourceFile().fileName()).isEqualTo("data-1");
        assertThat(sourceMeta.sourceFile().rowCount()).isEqualTo(3);
    }

    @Test
    void testBuildsMultiSourceArchiveWithContinuousRowIds() throws Exception {
        PkFullTextDataFileReader first = mock(PkFullTextDataFileReader.class);
        when(first.rowCount()).thenReturn(2L);
        when(first.readNextText())
                .thenReturn(BinaryString.fromString("first"), BinaryString.fromString("second"));
        PkFullTextDataFileReader second = mock(PkFullTextDataFileReader.class);
        when(second.rowCount()).thenReturn(1L);
        when(second.readNextText()).thenReturn(BinaryString.fromString("third"));
        RecordingIndexer indexer = new RecordingIndexer();

        IndexFileMeta archive =
                new PkFullTextIndexFile(LocalFileIO.create(), pathFactory())
                        .build(
                                Arrays.asList(
                                        new PkFullTextIndexFile.Source(
                                                dataFile("data-1", 2), first),
                                        new PkFullTextIndexFile.Source(
                                                dataFile("data-2", 1), second)),
                                new DataField(7, "content", DataTypes.STRING()),
                                indexer);

        assertThat(indexer.rowIds).containsExactly(0L, 1L, 2L);
        assertThat(indexer.values)
                .containsExactly(
                        BinaryString.fromString("first"),
                        BinaryString.fromString("second"),
                        BinaryString.fromString("third"));
        assertThat(archive.rowCount()).isEqualTo(3);
        assertThat(archive.globalIndexMeta().rowRangeEnd()).isEqualTo(2);
        PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(archive);
        assertThat(sourceMeta.sourceFiles())
                .extracting(source -> source.fileName() + ":" + source.rowCount())
                .containsExactly("data-1:2", "data-2:1");
    }

    @Test
    void testBuilderOpensTheFileReaderAndDelegatesEffectiveOptions() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        DataField textField = new DataField(7, "content", DataTypes.STRING());
        Options options = new Options();
        PkFullTextIndexFile indexFile = mock(PkFullTextIndexFile.class);
        PkFullTextDataFileReader.Factory readerFactory =
                mock(PkFullTextDataFileReader.Factory.class);
        PkFullTextDataFileReader reader = mock(PkFullTextDataFileReader.class);
        IndexFileMeta expected = mock(IndexFileMeta.class);
        when(readerFactory.create(source)).thenReturn(reader);
        when(indexFile.build(source, reader, textField, options)).thenReturn(expected);

        IndexFileMeta actual =
                new PkFullTextIndexBuilder(indexFile, readerFactory, textField, options)
                        .build(source);

        assertThat(actual).isSameAs(expected);
        verify(readerFactory).create(source);
        verify(indexFile).build(source, reader, textField, options);
    }

    @Test
    void testBuilderDelegatesOrderedSources() throws Exception {
        DataFileMeta first = dataFile("data-1", 2);
        DataFileMeta second = dataFile("data-2", 1);
        DataField textField = new DataField(7, "content", DataTypes.STRING());
        Options options = new Options();
        PkFullTextIndexFile indexFile = mock(PkFullTextIndexFile.class);
        PkFullTextDataFileReader.Factory readerFactory =
                mock(PkFullTextDataFileReader.Factory.class);
        when(readerFactory.create(first)).thenReturn(mock(PkFullTextDataFileReader.class));
        when(readerFactory.create(second)).thenReturn(mock(PkFullTextDataFileReader.class));
        IndexFileMeta expected = mock(IndexFileMeta.class);
        when(indexFile.build(anyList(), same(textField), same(options))).thenReturn(expected);

        IndexFileMeta actual =
                new PkFullTextIndexBuilder(indexFile, readerFactory, textField, options)
                        .build(Arrays.asList(first, second));

        assertThat(actual).isSameAs(expected);
        verify(readerFactory).create(first);
        verify(readerFactory).create(second);
        verify(indexFile).build(anyList(), same(textField), same(options));
    }

    @Test
    void testClosesReaderWhenIndexerCreationFails() throws Exception {
        PkFullTextDataFileReader reader = mock(PkFullTextDataFileReader.class);

        assertThatThrownBy(
                        () ->
                                new PkFullTextIndexFile(LocalFileIO.create(), pathFactory())
                                        .build(
                                                dataFile("data-1", 1),
                                                reader,
                                                new DataField(7, "content", DataTypes.STRING()),
                                                new Options()))
                .isInstanceOf(RuntimeException.class);

        verify(reader).close();
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(3);
    }

    private IndexPathFactory pathFactory() {
        Path directory = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(directory, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(directory, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    private static class RecordingIndexer implements GlobalIndexer {

        private final List<Object> values = new ArrayList<>();
        private final List<Long> rowIds = new ArrayList<>();

        @Override
        public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
            return new GlobalIndexSingleColumnWriter() {
                @Override
                public void write(Object key, long relativeRowId) {
                    values.add(key);
                    rowIds.add(relativeRowId);
                }

                @Override
                public List<ResultEntry> finish() {
                    try {
                        String fileName = fileWriter.newFileName("full-text");
                        try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                            out.write(1);
                        }
                        return Collections.singletonList(
                                new ResultEntry(fileName, rowIds.size(), new byte[] {2}));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        @Override
        public GlobalIndexReader createReader(
                GlobalIndexFileReader fileReader,
                List<GlobalIndexIOMeta> files,
                ExecutorService executor) {
            throw new UnsupportedOperationException();
        }
    }
}
