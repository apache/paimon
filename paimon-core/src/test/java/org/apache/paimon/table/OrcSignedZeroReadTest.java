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

package org.apache.paimon.table;

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Table reads for an ORC append file whose footer max is {@code -0.0} while a {@code +0.0} row is
 * still present. File stats and ORC SearchArgument both have to leave that row visible.
 */
class OrcSignedZeroReadTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"float", "double"})
    public void testPositiveZeroSurvivesNegativeZeroUpperBound(String typeName) throws Exception {
        boolean isFloat = "float".equals(typeName);
        DataType type = isFloat ? DataTypes.FLOAT() : DataTypes.DOUBLE();
        FileStoreTable table = createTable(type);
        writeFile(table, isFloat);
        writeFile(table, isFloat);

        assertThat(count(table, null)).isEqualTo(6);
        assertMatches(table, isFloat);

        compact(table);
        assertMatches(table, isFloat);
    }

    @Test
    public void testFiniteDoubleFilterStillPlans() throws Exception {
        FileStoreTable table = createTable(DataTypes.DOUBLE());
        writeFinite(table);
        Predicate filter = new PredicateBuilder(table.rowType()).equal(0, 1.0d);
        assertThat(count(table, filter)).isEqualTo(1);
    }

    private void assertMatches(FileStoreTable table, boolean isFloat) throws Exception {
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Object positiveZero;
        Object negativeZero;
        if (isFloat) {
            positiveZero = Float.valueOf(0.0f);
            negativeZero = -0.0f;
        } else {
            positiveZero = 0.0d;
            negativeZero = -0.0d;
        }
        assertThat(count(table, builder.equal(0, positiveZero))).as("equal(+0.0)").isEqualTo(2);
        assertThat(count(table, builder.greaterThan(0, negativeZero)))
                .as("greaterThan(-0.0)")
                .isEqualTo(2);
    }

    private FileStoreTable createTable(DataType type) throws Exception {
        Path path = new Path(tempDir.toString() + "/" + type.getTypeRoot().name());
        Map<String, String> options = new HashMap<>();
        options.put("file.format", "orc");
        options.put("bucket", "-1");
        options.put("async-file-write", "false");
        RowType rowType = new RowType(Collections.singletonList(new DataField(0, "v", type)));
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                ""));
        return new AppendOnlyFileStoreTable(LocalFileIO.create(), path, schema);
    }

    private void writeFile(FileStoreTable table, boolean isFloat) throws Exception {
        Object negativeOne;
        Object negativeZero;
        Object positiveZero;
        if (isFloat) {
            negativeOne = -1.0f;
            negativeZero = -0.0f;
            positiveZero = 0.0f;
        } else {
            negativeOne = -1.0d;
            negativeZero = -0.0d;
            positiveZero = 0.0d;
        }
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(negativeOne));
            write.write(GenericRow.of(negativeZero));
            write.write(GenericRow.of(positiveZero));
            commit.commit(write.prepareCommit());
        }
    }

    private void writeFinite(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1.0d));
            write.write(GenericRow.of(2.0d));
            commit.commit(write.prepareCommit());
        }
    }

    private int count(FileStoreTable table, Predicate filter) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        if (filter != null) {
            builder.withFilter(filter);
            assertThat(builder.newScan().plan().splits())
                    .as("filter planned no splits")
                    .isNotEmpty();
        }
        int[] matched = new int[1];
        try (RecordReader<InternalRow> reader =
                builder.newRead().executeFilter().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> matched[0]++);
        }
        return matched[0];
    }

    private void compact(FileStoreTable table) throws Exception {
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        assertThat(splits).isNotEmpty();
        List<DataFileMeta> files = new ArrayList<>();
        BinaryRow partition = ((DataSplit) splits.get(0)).partition();
        for (Split split : splits) {
            files.addAll(((DataSplit) split).dataFiles());
        }
        assertThat(files.size()).isGreaterThan(1);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            FileStoreWrite<?> fileStoreWrite = ((TableWriteImpl<?>) write).getWrite();
            CommitMessage message =
                    new AppendCompactTask(partition, files)
                            .doCompact(table, (BaseAppendFileStoreWrite) fileStoreWrite);
            commit.commit(Collections.singletonList(message));
        }
    }
}
