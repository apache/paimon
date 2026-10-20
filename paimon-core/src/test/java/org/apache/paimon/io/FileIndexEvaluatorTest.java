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
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileIndexEvaluator}. */
public class FileIndexEvaluatorTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"1,true", "1,false", "2,true", "2,false"})
    void testIgnoreUnknownIndex(int version, boolean embedded) throws Exception {
        Options options = new Options();
        assertThat(new CoreOptions(options).fileIndexReadIgnoreUnknownType()).isFalse();
        options.set(CoreOptions.FILE_INDEX_READ_IGNORE_UNKNOWN_TYPE, true);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(bytes, version)) {
            writer.writeIndex("a", "unknown", out -> out.write(42));
            writer.finish();
        }
        LocalFileIO fileIO = LocalFileIO.create();
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir.toUri()),
                        "avro",
                        "data-",
                        "changelog-",
                        false,
                        null,
                        null);
        String indexName = "data.index";
        if (!embedded) {
            try (OutputStream out =
                    fileIO.newOutputStream(new Path(pathFactory.parent(), indexName), false)) {
                out.write(bytes.toByteArray());
            }
        }
        DataFileMeta file =
                DataFileMeta.forAppend(
                        "data.avro",
                        0,
                        2,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        0,
                        embedded ? Collections.emptyList() : Collections.singletonList(indexName),
                        embedded ? bytes.toByteArray() : null,
                        null,
                        null,
                        null,
                        null,
                        null);
        List<Predicate> filters =
                Collections.singletonList(
                        new PredicateBuilder(tableSchema().logicalRowType()).equal(0, 1));
        assertThatThrownBy(
                        () ->
                                FileIndexEvaluator.evaluate(
                                        fileIO,
                                        tableSchema(),
                                        filters,
                                        null,
                                        null,
                                        pathFactory,
                                        file,
                                        null))
                .hasMessage("Can't find file index for type: unknown");
        assertThat(
                        FileIndexEvaluator.evaluate(
                                fileIO,
                                tableSchema(),
                                filters,
                                null,
                                null,
                                pathFactory,
                                file,
                                null,
                                0L,
                                new CoreOptions(options).fileIndexReadIgnoreUnknownType()))
                .isSameAs(FileIndexResult.REMAIN);
    }

    @Test
    public void testLimitIntersectsBitmapDeletionVector() throws Exception {
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        for (int position = 0; position < 5; position++) {
            deletionVector.delete(position);
        }

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        null,
                        Collections.emptyList(),
                        null,
                        10,
                        null,
                        DataFileTestUtils.newFile("data.avro", 0, 0, 19, 0L),
                        deletionVector);

        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get())
                .isEqualTo(RoaringBitmap32.bitmapOfRange(5, 15));
    }

    @Test
    public void testBitmap64DeletionVectorFallsBack() throws Exception {
        Bitmap64DeletionVector deletionVector = new Bitmap64DeletionVector();
        deletionVector.delete(0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        null,
                        Collections.emptyList(),
                        null,
                        10,
                        null,
                        DataFileTestUtils.newFile("data.avro", 0, 0, 19, 0L),
                        deletionVector);

        assertThat(result).isSameAs(FileIndexResult.REMAIN);
    }

    @Test
    public void testLargeFileFallsBack() throws Exception {
        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        null,
                        Collections.emptyList(),
                        null,
                        10,
                        null,
                        fileWithRowCount(Integer.MAX_VALUE + 2L),
                        null);

        assertThat(result).isSameAs(FileIndexResult.REMAIN);
    }

    @Test
    public void testDataFilterIntersectsDeletionVector() throws Exception {
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);

        assertThat(evaluateEmbeddedIndex(deletionVector, 0L)).isSameAs(FileIndexResult.SKIP);
    }

    @Test
    public void testFullyDeletedFileIsSkipped() throws Exception {
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);
        deletionVector.delete(1);

        assertThat(evaluateEmbeddedIndex(deletionVector, 0L)).isSameAs(FileIndexResult.SKIP);
    }

    @Test
    public void testDataFilterIntersectsOffsetDeletionVector() throws Exception {
        BitmapDeletionVector hit = new BitmapDeletionVector();
        hit.delete(10);
        assertThat(evaluateEmbeddedIndex(hit, 10L)).isSameAs(FileIndexResult.SKIP);

        BitmapDeletionVector neighbour = new BitmapDeletionVector();
        neighbour.delete(11);
        assertThat(evaluateEmbeddedIndex(neighbour, 10L).remain()).isTrue();
    }

    private static FileIndexResult evaluateEmbeddedIndex(
            BitmapDeletionVector deletionVector, long fileOffset) throws IOException {
        return FileIndexEvaluator.evaluate(
                null,
                tableSchema(),
                Collections.singletonList(
                        new PredicateBuilder(
                                        RowType.of(
                                                new DataType[] {DataTypes.INT()},
                                                new String[] {"a"}))
                                .equal(0, 1)),
                null,
                null,
                null,
                DataFileMeta.forAppend(
                        "file",
                        0,
                        2,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        embeddedBitmapIndex(),
                        null,
                        null,
                        null,
                        null,
                        null),
                deletionVector,
                fileOffset);
    }

    private static TableSchema tableSchema() {
        return new TableSchema(
                1,
                Collections.singletonList(new DataField(0, "a", DataTypes.INT())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static DataFileMeta fileWithRowCount(long rowCount) {
        return DataFileMeta.forAppend(
                "data.avro",
                0,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static byte[] embeddedBitmapIndex() throws IOException {
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(DataTypes.INT(), new Options());
        FileIndexWriter indexWriter = bitmapFileIndex.createWriter();
        indexWriter.write(1);
        indexWriter.write(2);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output, 1)) {
            writer.writeIndex("a", BitmapFileIndexFactory.BITMAP_INDEX, indexWriter::writeTo);
            writer.finish();
        }
        return output.toByteArray();
    }
}
