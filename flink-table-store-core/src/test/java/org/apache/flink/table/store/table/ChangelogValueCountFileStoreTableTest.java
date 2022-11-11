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
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.io.DataFilePathFactory;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogValueCountFileStoreTable}. */
public class ChangelogValueCountFileStoreTableTest extends FileStoreTableTestBase {

    @Test
    public void testBatchReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().plan().splits();
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "1|11|101|binary|varbinary",
                                "1|11|101|binary|varbinary",
                                "1|12|102|binary|varbinary"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "2|20|200|binary|varbinary",
                                "2|21|201|binary|varbinary",
                                "2|22|202|binary|varbinary"));
    }

    @Test
    public void testBatchProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().plan().splits();
        TableRead read = table.newRead().withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("101|11", "101|11", "102|12"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("200|20", "201|21", "202|22"));
    }

    @Test
    public void testBatchFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = builder.equal(2, 201L);
        List<Split> splits = table.newScan().withFilter(predicate).plan().splits();
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "2|21|201|binary|varbinary",
                                // this record is in the same file with "delete 2|21|201"
                                "2|22|202|binary|varbinary"));
    }

    @Test
    public void testStreamingReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().withIncremental(true).plan().splits();
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList("-1|10|100|binary|varbinary", "+1|11|101|binary|varbinary"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "-2|21|201|binary|varbinary",
                                "-2|21|201|binary|varbinary",
                                "+2|22|202|binary|varbinary"));
    }

    @Test
    public void testStreamingProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().withIncremental(true).plan().splits();
        TableRead read = table.newRead().withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("-100|10", "+101|11"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("-201|21", "-201|21", "+202|22"));
    }

    @Test
    public void testStreamingFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = builder.equal(2, 201L);
        List<Split> splits =
                table.newScan().withIncremental(true).withFilter(predicate).plan().splits();
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "-2|21|201|binary|varbinary",
                                "-2|21|201|binary|varbinary",
                                // this record is in the same file with "delete 2|21|201"
                                "+2|22|202|binary|varbinary"));
    }

    private void writeData() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(1, 11, 101L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 21, 201L));
        write.write(rowData(1, 12, 102L));
        write.write(rowData(2, 21, 201L));
        write.write(rowData(2, 21, 201L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 11, 101L));
        write.write(rowData(2, 22, 202L));
        write.write(rowDataWithKind(RowKind.DELETE, 2, 21, 201L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 100L));
        write.write(rowDataWithKind(RowKind.DELETE, 2, 21, 201L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
    }

    @Test
    public void testChangelogWithoutDataFile() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        // no data file should be produced from this commit
        write.write(rowData(1, 10, 100L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();

        // check that no data file is produced
        List<Split> splits = table.newScan().withIncremental(true).plan().splits();
        assertThat(splits).isEmpty();
        // check that no changelog file is produced
        Path bucketPath = DataFilePathFactory.bucketPath(table.location(), "1", 0);
        assertThat(bucketPath.getFileSystem().listStatus(bucketPath)).isNullOrEmpty();
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Configuration> configure)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.WRITE_MODE, WriteMode.CHANGE_LOG);
        configure.accept(conf);
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return new ChangelogValueCountFileStoreTable(tablePath, schemaManager, tableSchema);
    }
}
