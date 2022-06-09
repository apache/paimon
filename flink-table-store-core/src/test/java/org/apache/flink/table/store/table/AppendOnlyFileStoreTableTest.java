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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.predicate.Literal;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AppendOnlyFileStoreTable}. */
// TODO enable this test class after append only file store with avro format is fixed
@Disabled
public class AppendOnlyFileStoreTableTest extends FileStoreTableTestBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testBatchReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan(false).plan().splits;
        TableRead read = table.newRead(false);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList("1|10|100", "1|11|101", "1|12|102", "1|11|101", "1|12|102"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("2|20|200", "2|21|201", "2|22|202", "2|21|201"));
    }

    @Test
    public void testBatchProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan(false).plan().splits;
        TableRead read = table.newRead(false).withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("100|10", "101|11", "102|12", "101|11", "102|12"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("200|20", "201|21", "202|22", "201|21"));
    }

    @Test
    public void testBatchFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        Predicate predicate =
                PredicateBuilder.equal(
                        2, Literal.fromJavaObject(DataTypes.BIGINT().getLogicalType(), 201L));
        List<Split> splits = table.newScan(false).withFilter(predicate).plan().splits;
        TableRead read = table.newRead(false);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "2|21|201",
                                // this record is in the same file with the first "2|21|201"
                                "2|22|202",
                                "2|21|201"));
    }

    @Test
    public void testStreamingReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan(true).plan().splits;
        TableRead read = table.newRead(true);
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("+1|11|101", "+1|12|102"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("+2|21|201"));
    }

    @Test
    public void testStreamingProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan(true).plan().splits;
        TableRead read = table.newRead(true).withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .hasSameElementsAs(Arrays.asList("+101|11", "+102|12"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("+201|21"));
    }

    @Test
    public void testStreamingFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        Predicate predicate =
                PredicateBuilder.equal(
                        2, Literal.fromJavaObject(DataTypes.BIGINT().getLogicalType(), 101L));
        List<Split> splits = table.newScan(true).withFilter(predicate).plan().splits;
        TableRead read = table.newRead(true);
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "+1|11|101",
                                // this record is in the same file with "+1|11|101"
                                "+1|12|102"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING)).isEmpty();
    }

    private void writeData() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(false);

        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(2, 20, 200L));
        write.write(GenericRowData.of(1, 11, 101L));
        table.newCommit(null).commit("0", write.prepareCommit());

        write.write(GenericRowData.of(1, 12, 102L));
        write.write(GenericRowData.of(2, 21, 201L));
        write.write(GenericRowData.of(2, 22, 202L));
        table.newCommit(null).commit("1", write.prepareCommit());

        write.write(GenericRowData.of(1, 11, 101L));
        write.write(GenericRowData.of(2, 21, 201L));
        write.write(GenericRowData.of(1, 12, 102L));
        table.newCommit(null).commit("2", write.prepareCommit());

        write.close();
    }

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Path tablePath = new Path(tempDir.toString());
        Configuration conf = new Configuration();
        conf.set(FileStoreOptions.PATH, tablePath.toString());
        conf.set(FileStoreOptions.FILE_FORMAT, "avro");
        conf.set(FileStoreOptions.WRITE_MODE, WriteMode.APPEND_ONLY);
        Schema schema =
                new SchemaManager(tablePath)
                        .commitNewVersion(
                                new UpdateSchema(
                                        ROW_TYPE,
                                        Collections.singletonList("pt"),
                                        Collections.emptyList(),
                                        conf.toMap(),
                                        ""));
        return new AppendOnlyFileStoreTable(tablePath.getName(), schema, "user");
    }
}
