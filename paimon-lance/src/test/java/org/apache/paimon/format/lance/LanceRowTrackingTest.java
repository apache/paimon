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

package org.apache.paimon.format.lance;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.DataEvolutionTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for Lance file format with row tracking and data evolution. */
public class LanceRowTrackingTest extends DataEvolutionTestBase {

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
        Options options = new Options();
        options.set(WAREHOUSE, warehouse.toUri().toString());
        CatalogContext context = CatalogContext.create(options, new TraceableFileIO.Loader(), null);
        catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase(database, true);
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "lance");
        return schemaBuilder.build();
    }

    @Test
    public void testBasicWriteRead() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        // Access row data inline within forEachRemaining. Arrow-backed ColumnarRow is a
        // mutable view whose underlying off-heap memory is freed when the reader is closed
        // (which happens inside forEachRemaining via ConcatRecordReader).
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2).toString()).isEqualTo("b");
                    count.incrementAndGet();
                });
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    public void testAutoRowIdAssignment() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < 5; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("b" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 5; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                BinaryString.fromString("b" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int idx = r.getInt(0);
                    assertThat(r.getString(1).toString()).isEqualTo("a" + idx);
                    assertThat(r.getString(2).toString()).isEqualTo("b" + idx);
                    count.incrementAndGet();
                });
        assertThat(count.get()).isEqualTo(10);
    }

    @Test
    public void testDataEvolutionBasic() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write.prepareCommit());
        }

        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2).toString()).isEqualTo("c"); // overwritten
                });
    }

    @Test
    public void testOnlySomeColumns() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Collections.singletonList("f0"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f1"));
        RowType writeType2 = schema.rowType().project(Collections.singletonList("f2"));

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1));
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("a")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write2 = builder.newWrite().withWriteType(writeType2)) {
            write2.write(GenericRow.of(BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write2.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2)).isEqualTo(BinaryString.fromString("b"));
                });
    }

    @Test
    public void testMultipleRows() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            for (int i = 0; i < 10; i++) {
                write0.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        long rowId = getTableDefault().snapshotManager().latestSnapshot().nextRowId() - 10;
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            for (int i = 0; i < 10; i++) {
                write1.write(GenericRow.of(BinaryString.fromString("b" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, rowId);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger rowCount = new AtomicInteger(0);
        reader.forEachRemaining(r -> rowCount.incrementAndGet());
        assertThat(rowCount.get()).isEqualTo(10);
    }
}
