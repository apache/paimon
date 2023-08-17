package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** this is a doc. */
public class CompactActionForDbITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private Catalog catalog;

    @Test
    public void testBatchCompact() throws Exception {
        Map<String, String> options = new HashMap<>();
        //        options.put(CoreOptions.WRITE_ONLY.key(), "true");

        createDatabase();

        FileStoreTable table1 =
                createFileStoreTable(
                        "table1",
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        FileStoreTable table2 =
                createFileStoreTable(
                        "table2",
                        ROW_TYPE,
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        options);
        SnapshotManager snapshotManager1 = table1.snapshotManager();
        String commitUser1 = UUID.randomUUID().toString();
        StreamWriteBuilder streamWriteBuilder1 =
                table1.newStreamWriteBuilder().withCommitUser(commitUser1);
        StreamTableWrite write1 = streamWriteBuilder1.newWrite();
        StreamTableCommit commit1 = streamWriteBuilder1.newCommit();

        write1.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write1.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write1.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit1.commit(0, write1.prepareCommit(true, 0));

        write1.write(rowData(2, 100, 15, BinaryString.fromString("20221208")));
        write1.write(rowData(2, 100, 16, BinaryString.fromString("20221208")));
        write1.write(rowData(2, 100, 15, BinaryString.fromString("20221209")));
        commit1.commit(1, write1.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager1.snapshot(snapshotManager1.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        // ******************************** table2

        SnapshotManager snapshotManager2 = table2.snapshotManager();
        String commitUser2 = UUID.randomUUID().toString();
        StreamWriteBuilder streamWriteBuilder2 =
                table2.newStreamWriteBuilder().withCommitUser(commitUser2);
        StreamTableWrite write2 = streamWriteBuilder2.newWrite();
        StreamTableCommit commit2 = streamWriteBuilder2.newCommit();

        write2.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write2.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write2.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit2.commit(0, write2.prepareCommit(true, 0));

        write2.write(rowData(2, 100, 15, BinaryString.fromString("20221208")));
        write2.write(rowData(2, 100, 16, BinaryString.fromString("20221208")));
        write2.write(rowData(2, 100, 15, BinaryString.fromString("20221209")));
        commit2.commit(1, write2.prepareCommit(true, 1));

        Snapshot snapshot2 = snapshotManager2.snapshot(snapshotManager2.latestSnapshotId());
        assertThat(snapshot2.id()).isEqualTo(2);
        assertThat(snapshot2.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(ThreadLocalRandom.current().nextInt(2) + 1);
        new CompactActionForDb(
                        warehouse,
                        database,
                        table1.bucketMode(),
                        Options.fromMap(table1.options()),
                        table1.coreOptions())
                .build(env);
        env.execute();

        // source 端没有问题
        // +I 2|20221208|15|0|0|default|table1
        // +I 2|20221209|15|0|0|default|table1
        // +I 2|20221208|16|0|0|default|table1
        // +I 2|20221208|15|0|0|default|table2
        // +I 2|20221209|15|0|0|default|table2
        // +I 2|20221208|16|0|0|default|table2

        snapshot = snapshotManager1.snapshot(snapshotManager1.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table1.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }

        snapshot2 = snapshotManager2.snapshot(snapshotManager2.latestSnapshotId());
        assertThat(snapshot2.id()).isEqualTo(3);
        assertThat(snapshot2.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        splits = table2.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(3);
        for (DataSplit split : splits) {
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }
    }

    protected void createDatabase() throws Catalog.DatabaseAlreadyExistException {
        catalog = catalog();
        catalog.createDatabase(database, true);
    }

    protected FileStoreTable createFileStoreTable(
            String tableName,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options)
            throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(
                identifier,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""),
                false);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
