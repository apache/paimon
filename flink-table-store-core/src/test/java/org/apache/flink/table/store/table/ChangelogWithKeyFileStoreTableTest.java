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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.WriteMode;
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
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogWithKeyFileStoreTable}. */
public class ChangelogWithKeyFileStoreTableTest extends FileStoreTableTestBase {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSequenceNumber() throws Exception {
        FileStoreTable table =
                createFileStoreTable(conf -> conf.set(CoreOptions.SEQUENCE_FIELD, "b"));
        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");
        write.write(GenericRowData.of(1, 10, 200L));
        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(1, 11, 101L));
        commit.commit("0", write.prepareCommit());
        write.write(GenericRowData.of(1, 11, 55L));
        commit.commit("1", write.prepareCommit());
        write.close();

        List<Split> splits = table.newScan().plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("1|10|100", "1|11|101", "1|10|200", "1|11|55"));
    }

    @Test
    public void testBatchReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(Collections.singletonList("1|10|1000"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("2|20|200", "2|21|20001", "2|22|202", "2|21|2001"));
    }

    @Test
    public void testBatchProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().plan().splits;
        TableRead read = table.newRead().withProjection(PROJECTION);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Collections.singletonList("1000|10"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("200|20", "20001|21", "202|22", "2001|21"));
    }

    @Test
    public void testBatchFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = PredicateBuilder.and(builder.equal(2, 201L), builder.equal(1, 21));
        List<Split> splits = table.newScan().withFilter(predicate).plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                // only filter on key should be performed,
                                // and records from the same file should also be selected
                                "2|21|20001", "2|22|202", "2|21|2001"));
    }

    @Test
    public void testStreamingReadWrite() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().withIncremental(true).plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(Collections.singletonList("-1|11|1001"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("+2|21|20001", "+2|22|202", "-2|20|200"));
    }

    @Test
    public void testStreamingProjection() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();

        List<Split> splits = table.newScan().withIncremental(true).plan().splits;
        TableRead read = table.newRead().withProjection(PROJECTION);

        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Collections.singletonList("-1001|11"));
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_PROJECTED_ROW_TO_STRING))
                .isEqualTo(Arrays.asList("+20001|21", "+202|22", "-200|20"));
    }

    @Test
    public void testStreamingFilter() throws Exception {
        writeData();
        FileStoreTable table = createFileStoreTable();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        Predicate predicate = PredicateBuilder.and(builder.equal(2, 201L), builder.equal(1, 21));
        List<Split> splits =
                table.newScan().withIncremental(true).withFilter(predicate).plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, STREAMING_ROW_TO_STRING)).isEmpty();
        assertThat(getResult(read, splits, binaryRow(2), 0, STREAMING_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                // only filter on key should be performed,
                                // and records from the same file should also be selected
                                "+2|21|20001", "+2|22|202", "-2|20|200"));
    }

    @Test
    public void testStreamingChangelog() throws Exception {
        FileStoreTable table = createFileStoreTable(true);
        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");
        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 1, 10, 100L));
        write.write(GenericRowData.of(1, 10, 101L));
        write.write(GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 1, 10, 101L));
        write.write(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, 10, 102L));
        commit.commit("0", write.prepareCommit());
        write.close();

        List<Split> splits = table.newScan().withIncremental(true).plan().splits;
        TableRead read = table.newRead();
        assertThat(getResult(read, splits, binaryRow(1), 0, CHANGELOG_ROW_TO_STRING))
                .isEqualTo(
                        Arrays.asList(
                                "+I 1|10|100",
                                "-D 1|10|100",
                                "+I 1|10|101",
                                "-U 1|10|101",
                                "+U 1|10|102"));
    }

    private void writeData() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite();
        TableCommit commit = table.newCommit("user");

        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(2, 20, 200L));
        write.write(GenericRowData.of(1, 11, 101L));
        commit.commit("0", write.prepareCommit());

        write.write(GenericRowData.of(1, 10, 1000L));
        write.write(GenericRowData.of(2, 21, 201L));
        write.write(GenericRowData.of(2, 21, 2001L));
        commit.commit("1", write.prepareCommit());

        write.write(GenericRowData.of(1, 11, 1001L));
        write.write(GenericRowData.of(2, 21, 20001L));
        write.write(GenericRowData.of(2, 22, 202L));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 1, 11, 1001L));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 2, 20, 200L));
        commit.commit("2", write.prepareCommit());

        write.close();
    }

    protected FileStoreTable createFileStoreTable(boolean changelogFile) throws Exception {
        return createFileStoreTable(conf -> conf.set(CoreOptions.CHANGELOG_FILE, changelogFile));
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Configuration> configure)
            throws Exception {
        Path tablePath = new Path(tempDir.toString());
        Configuration conf = new Configuration();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.FILE_FORMAT, "avro");
        conf.set(CoreOptions.WRITE_MODE, WriteMode.CHANGE_LOG);
        configure.accept(conf);
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                conf.toMap(),
                                ""));
        return new ChangelogWithKeyFileStoreTable(tablePath, schemaManager, tableSchema);
    }
}
