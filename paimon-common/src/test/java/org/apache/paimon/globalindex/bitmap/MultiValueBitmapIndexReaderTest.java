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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the bitmap-backed multivalue global index. */
class MultiValueBitmapIndexReaderTest {

    private final DataType arrayType = DataTypes.ARRAY(DataTypes.STRING());
    private final DataField dataField = new DataField(1, "tags", arrayType);
    private final FieldRef fieldRef = new FieldRef(1, "tags", arrayType);

    private FileIO fileIO;
    private Path basePath;
    private GlobalIndexFileWriter fileWriter;
    private GlobalIndexFileReader fileReader;
    private GlobalIndexer globalIndexer;

    @TempDir java.nio.file.Path tempPath;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
        basePath = new Path(tempPath.toUri());
        fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID() + ".index";
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(new Path(basePath, fileName), true);
                    }
                };
        fileReader = meta -> fileIO.newInputStream(meta.filePath());
        globalIndexer = new MultiValueGlobalIndexer(dataField, new Options());
    }

    @Test
    void testArrayContainsAndSafeFallback() throws Exception {
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) globalIndexer.createWriter(fileWriter);
        writer.write(str("A"), 0);
        writer.write(str("A"), 0);
        writer.write(str("B"), 0);
        writer.write(str("B"), 3);
        writer.write(str("C"), 4);

        ResultEntry result = writer.finish(5).get(0);
        assertThat(result.rowCount()).isEqualTo(5);
        Path path = new Path(basePath, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, fileIO.getFileSize(path), result.meta());
        assertThat(
                        MultiValueIndexFileMeta.hasCompatibleElementType(
                                result.meta(), DataTypes.STRING()))
                .isTrue();
        assertThat(
                        MultiValueIndexFileMeta.hasCompatibleElementType(
                                result.meta(), DataTypes.BIGINT()))
                .isFalse();

        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader,
                        Collections.singletonList(meta),
                        5,
                        newDirectExecutorService())) {
            assertRows(reader.visitArrayContains(fieldRef, str("A")).join(), 0L);
            assertRows(reader.visitArrayContains(fieldRef, str("B")).join(), 0L, 3L);
            assertRows(reader.visitArrayContains(fieldRef, str("missing")).join());
            assertRows(reader.visitArrayContains(fieldRef, null).join());
            assertThat(
                            reader.visitArrayContains(
                                            new FieldRef(
                                                    1, "tags", DataTypes.ARRAY(DataTypes.BIGINT())),
                                            1L)
                                    .join())
                    .isEmpty();
            assertThat(reader.visitIsNull(fieldRef).join()).isEmpty();
            assertThat(reader.visitIsNotNull(fieldRef).join()).isEmpty();

            assertThat(reader.visitEqual(fieldRef, array("A")).join()).isEmpty();
            assertThat(reader.visitContains(fieldRef, str("A")).join()).isEmpty();
        }

        GlobalIndexIOMeta legacyMeta =
                new GlobalIndexIOMeta(
                        path,
                        fileIO.getFileSize(path),
                        SortedIndexFileMeta.deserialize(result.meta()).serialize());
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader,
                        Collections.singletonList(legacyMeta),
                        5,
                        newDirectExecutorService())) {
            assertThat(reader.visitArrayContains(fieldRef, str("A")).join()).isEmpty();
        }
    }

    @Test
    void testRowsWithoutIndexableElementsStillProduceAnIndex() throws Exception {
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) globalIndexer.createWriter(fileWriter);

        ResultEntry result = writer.finish(3).get(0);
        assertThat(result.rowCount()).isEqualTo(3);
        Path path = new Path(basePath, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, fileIO.getFileSize(path), result.meta());
        try (GlobalIndexReader reader =
                globalIndexer.createReader(
                        fileReader,
                        Collections.singletonList(meta),
                        3,
                        newDirectExecutorService())) {
            assertRows(reader.visitArrayContains(fieldRef, str("A")).join());
        }
    }

    @Test
    void testNumericKeysUseLogicalOrder() throws Exception {
        DataType intArrayType = DataTypes.ARRAY(DataTypes.INT());
        DataField intField = new DataField(2, "numbers", intArrayType);
        FieldRef intFieldRef = new FieldRef(2, "numbers", intArrayType);
        GlobalIndexer intIndexer = new MultiValueGlobalIndexer(intField, new Options());
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) intIndexer.createWriter(fileWriter);
        writer.write(-1, 0);
        writer.write(0, 0);
        writer.write(1, 1);

        ResultEntry result = writer.finish(2).get(0);
        Path path = new Path(basePath, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, fileIO.getFileSize(path), result.meta());
        try (GlobalIndexReader reader =
                intIndexer.createReader(
                        fileReader,
                        Collections.singletonList(meta),
                        2,
                        newDirectExecutorService())) {
            assertRows(reader.visitArrayContains(intFieldRef, -1).join(), 0L);
            assertRows(reader.visitArrayContains(intFieldRef, 0).join(), 0L);
            assertRows(reader.visitArrayContains(intFieldRef, 1).join(), 1L);
        }
    }

    @Test
    void testFactoryAndTypeValidation() throws Exception {
        MultiValueGlobalIndexerFactory factory = new MultiValueGlobalIndexerFactory();
        assertThat(factory.identifier()).isEqualTo(MultiValueGlobalIndexerFactory.IDENTIFIER);
        assertThat(factory.create(dataField, new Options()))
                .isInstanceOf(MultiValueGlobalIndexer.class);
        assertThat(globalIndexer).isInstanceOf(SortedGlobalIndexer.class);
        GlobalIndexKeyExtractor extractor = ((SortedGlobalIndexer) globalIndexer).keyExtractor();
        assertThat(extractor.keyType()).isEqualTo(DataTypes.STRING());
        List<Object> extracted = new ArrayList<>();
        extractor.extract(array("B", null, "A"), extracted::add);
        assertThat(extracted).containsExactly(str("B"), str("A"));
        assertThatThrownBy(
                        () ->
                                new MultiValueGlobalIndexer(
                                        new DataField(2, "scalar", DataTypes.INT()), new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ARRAY");
        assertThatThrownBy(
                        () ->
                                new MultiValueGlobalIndexer(
                                        new DataField(
                                                3,
                                                "nested",
                                                DataTypes.ARRAY(
                                                        RowType.of(
                                                                new DataField(
                                                                        4,
                                                                        "value",
                                                                        DataTypes.INT())))),
                                        new Options()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("not supported by global index");
    }

    @Test
    void testRejectsUnsortedNormalizedKeys() throws Exception {
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) globalIndexer.createWriter(fileWriter);
        writer.write(str("B"), 0);
        assertThatThrownBy(() -> writer.write(str("A"), 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("monotonically increasing");
    }

    private static GenericArray array(String... values) {
        BinaryString[] strings = new BinaryString[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = values[i] == null ? null : str(values[i]);
        }
        return new GenericArray(strings);
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private static void assertRows(java.util.Optional<GlobalIndexResult> result, Long... expected) {
        assertThat(result).isPresent();
        Iterator<Long> iterator = result.get().results().iterator();
        List<Long> actual = new ArrayList<>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }
        assertThat(actual).containsExactlyInAnyOrder(expected);
    }
}
