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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Chain table reads must agree with the underlying branch when deletion vectors are enabled. */
public class ChainTableDeletionVectorReadTest {

    private static final String SNAPSHOT_BRANCH = "snapshot";
    private static final String DELTA_BRANCH = "delta";

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private final LocalFileIO fileIO = LocalFileIO.create();
    private long commitId = 0;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testChainReadMatchesBranchReadWithLateLowerSequenceInsert(boolean deletionVectors)
            throws Exception {
        createTable(deletionVectors);
        FileStoreTable chain = load();
        FileStoreTable snapshot = chain.switchToBranch(SNAPSHOT_BRANCH);

        write(snapshot, row(RowKind.INSERT, 4L, 5L, "v3"));
        write(snapshot, row(RowKind.INSERT, 2L, 6L, "v6"));
        write(snapshot, row(RowKind.INSERT, 4L, 2L, "v10"));

        List<String> branchRead = read(snapshot);
        List<String> chainRead = read(load());

        assertThat(branchRead)
                .containsExactly("+I(2,6,v6,CN,20250810,20)", "+I(4,5,v3,CN,20250810,20)");
        assertThat(chainRead).isEqualTo(branchRead);
    }

    private void createTable(boolean deletionVectors) throws Exception {
        tablePath = new Path("file://" + tempDir + "/chain_dv_" + deletionVectors);
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.BUCKET_KEY, "k");
        options.set(CoreOptions.SEQUENCE_FIELD, "seq");
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.DEDUPLICATE);
        options.set(CoreOptions.CHAIN_TABLE_ENABLED, true);
        options.set(CoreOptions.PARTITION_TIMESTAMP_PATTERN, "$dt $hour:00:00");
        options.set(CoreOptions.PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd HH:mm:ss");
        options.set(CoreOptions.CHAIN_TABLE_CHAIN_PARTITION_KEYS, "dt,hour");
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, deletionVectors);

        Schema schema =
                new Schema(
                        RowType.of(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {"k", "seq", "v", "region", "dt", "hour"})
                                .getFields(),
                        Arrays.asList("region", "dt", "hour"),
                        Arrays.asList("region", "dt", "hour", "k"),
                        options.toMap(),
                        "");
        schemaManager.createTable(schema);
        FileStoreTable table = load();
        table.createBranch(SNAPSHOT_BRANCH);
        table.createBranch(DELTA_BRANCH);
        configureBranch("main");
        configureBranch(SNAPSHOT_BRANCH);
        configureBranch(DELTA_BRANCH);
    }

    private void configureBranch(String branchName) throws Exception {
        SchemaManager sm = new SchemaManager(fileIO, tablePath, branchName);
        sm.commitChanges(
                SchemaChange.setOption(
                        CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), SNAPSHOT_BRANCH),
                SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), DELTA_BRANCH));
    }

    private FileStoreTable load() {
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        String branchName = CoreOptions.branch(options.toMap());
        TableSchema schema = new SchemaManager(fileIO, tablePath, branchName).latest().get();
        return FileStoreTableFactory.create(fileIO, tablePath, schema, CatalogEnvironment.empty());
    }

    private void write(FileStoreTable table, GenericRow rowData) throws Exception {
        try (InnerTableWrite write =
                table.newWrite("u").withIOManager(new IOManagerImpl(tempDir.toString()))) {
            write.write(rowData);
            long id = commitId++;
            try (StreamTableCommit commit = table.newCommit("u")) {
                List<CommitMessage> messages = write.prepareCommit(true, id);
                commit.commit(id, messages);
            }
        }
    }

    private List<String> read(FileStoreTable table) throws Exception {
        Map<String, String> filter = new HashMap<>();
        filter.put("dt", "20250810");
        filter.put("hour", "20");
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.withPartitionFilter(filter).newScan().plan();
        List<String> result = new ArrayList<>();
        InternalRowSerializer serializer =
                new InternalRowSerializer(table.schema().logicalRowType());
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(r -> result.add(serializer.copy(r).toString()));
        }
        result.sort(String::compareTo);
        return result;
    }

    private static GenericRow row(RowKind kind, Object k, Object seq, String v) {
        return GenericRow.ofKind(
                kind,
                k,
                seq,
                BinaryString.fromString(v),
                BinaryString.fromString("CN"),
                BinaryString.fromString("20250810"),
                BinaryString.fromString("20"));
    }
}
