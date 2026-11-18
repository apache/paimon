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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.testfulltext.TestFullTextGlobalIndexerFactory;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Spatial index candidates must be fully refined before taking ranked-search slots. */
class RTreeRankedFilterTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("geom", DataTypes.GEOMETRY("EPSG:3857"))
                .column("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                .column("content", DataTypes.STRING())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option("test.vector.dimension", "2")
                .option("test.vector.metric", "l2")
                .build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testVectorTopKWithUnsupportedSameFieldConjunct(boolean refine) throws Exception {
        FileStoreTable table = prepare(refine);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate bbox = builder.bboxIntersects(1, -1, -1, 2, 2);
        assertThat(vector(table, bbox).results()).containsExactly(0L);
        Predicate filter = PredicateBuilder.and(bbox, builder.equal(1, point(1, 1)));
        assertThat(vector(table, filter).results())
                .containsExactly(refine ? new Long[] {1L} : new Long[0]);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFullTextTopKWithUnsupportedSameFieldConjunct(boolean refine) throws Exception {
        FileStoreTable table = prepare(refine);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate bbox = builder.bboxIntersects(1, -1, -1, 2, 2);
        assertThat(fullText(table, bbox).results()).containsExactly(0L);
        Predicate filter = PredicateBuilder.and(bbox, builder.equal(1, point(1, 1)));
        assertThat(fullText(table, filter).results())
                .containsExactly(refine ? new Long[] {1L} : new Long[0]);
    }

    private FileStoreTable prepare(boolean refine) throws Exception {
        createTableDefault();
        FileStoreTable table =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_FILTER_REFINE_FROM_DATA.key(),
                                        Boolean.toString(refine)));
        Object[] geometries = {point(0, 0), point(1, 1), point(20, 20)};
        float[][] vectors = {{1, 0}, {0.6f, 0.8f}, {-1, 0}};
        String[] documents = {"paimon lake", "paimon", "other"};
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < geometries.length; i++) {
                writer.write(
                        GenericRow.of(
                                i,
                                geometries[i],
                                new GenericArray(vectors[i]),
                                BinaryString.fromString(documents[i])));
            }
            commit.commit(writer.prepareCommit());
        }
        buildIndex(table, "rtree", "geom", geometries);
        buildIndex(table, TestVectorGlobalIndexerFactory.IDENTIFIER, "vec", vectors);
        buildIndex(table, TestFullTextGlobalIndexerFactory.IDENTIFIER, "content", documents);
        return table;
    }

    private static GlobalIndexResult vector(FileStoreTable table, Predicate filter) {
        return table.newVectorSearchBuilder()
                .withVectorColumn("vec")
                .withVector(new float[] {1, 0})
                .withFilter(filter)
                .withLimit(1)
                .executeLocal();
    }

    private static GlobalIndexResult fullText(FileStoreTable table, Predicate filter) {
        return table.newFullTextSearchBuilder()
                .withQuery("content", "{\"match\":{\"query\":\"paimon lake\"}}")
                .withFilter(filter)
                .withLimit(1)
                .executeLocal();
    }

    private static void buildIndex(
            FileStoreTable table, String type, String column, Object[] values) throws Exception {
        DataField field = table.rowType().getField(column);
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexBuilderUtils.createIndexWriter(
                                table,
                                type,
                                Collections.singletonList(field),
                                table.coreOptions().toConfiguration());
        try {
            for (int i = 0; i < values.length; i++) {
                writer.write(values[i], i);
            }
            List<IndexFileMeta> files =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                            table.fileIO(),
                            table.store().pathFactory().globalIndexFileFactory(),
                            table.coreOptions(),
                            new Range(0, values.length - 1),
                            field.id(),
                            type,
                            writer.finish());
            try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
                commit.commit(
                        Collections.singletonList(
                                new CommitMessageImpl(
                                        BinaryRow.EMPTY_ROW,
                                        0,
                                        null,
                                        DataIncrement.indexIncrement(files),
                                        CompactIncrement.emptyIncrement())));
            }
        } finally {
            if (writer instanceof Closeable) {
                ((Closeable) writer).close();
            }
        }
    }

    private static byte[] point(double x, double y) {
        return ByteBuffer.allocate(21)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1)
                .putInt(1)
                .putDouble(x)
                .putDouble(y)
                .array();
    }
}
