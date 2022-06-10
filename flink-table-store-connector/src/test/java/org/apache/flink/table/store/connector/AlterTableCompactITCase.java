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

package org.apache.flink.table.store.connector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.utils.SnapshotFinder;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.file.FileStoreOptions.relativeTablePath;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.SINGLE_PARTITIONED;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for 'ALTER TABLE ... COMPACT'. */
public class AlterTableCompactITCase extends FileStoreTableITCase {

    private TestKeyValueGenerator generator;

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T0 (\n"
                        + "shopId INT\n, "
                        + "orderId BIGINT NOT NULL\n, "
                        + "itemId BIGINT)",
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "dt STRING\n, "
                        + "shopId INT\n, "
                        + "orderId BIGINT NOT NULL\n, "
                        + "itemId BIGINT)\n "
                        + "PARTITIONED BY (dt)",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "dt STRING\n, "
                        + "hr INT\n, "
                        + "shopId INT\n, "
                        + "orderId BIGINT NOT NULL\n, "
                        + "itemId BIGINT)"
                        + "PARTITIONED BY (dt, hr)");
    }

    @Test
    public void testNonPartitioned() throws IOException {
        generator = new TestKeyValueGenerator(NON_PARTITIONED);
        Random random = new Random();
        innerTest("T0", random.nextInt(10) + 1, NON_PARTITIONED);
    }

    @Test
    public void testSinglePartitioned() throws IOException {
        generator = new TestKeyValueGenerator(SINGLE_PARTITIONED);
        Random random = new Random();
        innerTest("T1", random.nextInt(10) + 1, SINGLE_PARTITIONED);
    }

    @Test
    public void testMultiPartitioned() throws IOException {
        generator = new TestKeyValueGenerator(MULTI_PARTITIONED);
        Random random = new Random();
        innerTest("T2", random.nextInt(10) + 1, MULTI_PARTITIONED);
    }

    private void innerTest(String tableName, int batchNum, TestKeyValueGenerator.GeneratorMode mode)
            throws IOException {
        // increase trigger to avoid auto-compaction
        batchSql(
                String.format(
                        "ALTER TABLE %s SET ('num-sorted-run.compaction-trigger' = '50')",
                        tableName));
        batchSql(
                String.format(
                        "ALTER TABLE %s SET ('num-sorted-run.stop-trigger' = '50')", tableName));

        Random random = new Random();
        List<KeyValue> dataset = new ArrayList<>();
        long latestSnapshot = 0L;
        for (int i = 0; i < batchNum; i++) {
            List<KeyValue> data = generateData(random.nextInt(200) + 1);
            String insertQuery =
                    String.format(
                            "INSERT INTO %s VALUES \n%s",
                            tableName,
                            data.stream()
                                    .map(kv -> kvAsString(kv, mode))
                                    .collect(Collectors.joining(",\n")));
            batchSql(insertQuery);
            Snapshot snapshot = findLatestSnapshot(tableName);
            assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            latestSnapshot = snapshot.id();
            dataset.addAll(data);
        }
        if (mode == NON_PARTITIONED) {
            String compactQuery = String.format("ALTER TABLE %s COMPACT", tableName);
            String selectQuery = String.format("SELECT * FROM %s", tableName);
            compactAndCheck(
                    tableName,
                    compactQuery,
                    selectQuery,
                    latestSnapshot,
                    dataset.stream()
                            .map(kv -> convertToRow(kv, mode))
                            .collect(Collectors.toList()));
        } else {
            List<BinaryRowData> partitions =
                    dataset.stream()
                            .map(kv -> generator.getPartition(kv))
                            .distinct()
                            .collect(Collectors.toList());
            while (!partitions.isEmpty()) {
                BinaryRowData part = pickPartition(partitions);
                Map<String, String> partSpec = TestKeyValueGenerator.toPartitionMap(part, mode);
                String compactQuery =
                        String.format(
                                "ALTER TABLE %s PARTITION (%s) COMPACT",
                                tableName, partAsString(partSpec, false));
                String selectQuery =
                        String.format(
                                "SELECT * FROM %s WHERE %s",
                                tableName, partAsString(partSpec, true));
                compactAndCheck(
                        tableName,
                        compactQuery,
                        selectQuery,
                        latestSnapshot,
                        dataset.stream()
                                .filter(kv -> partFilter(kv, part, mode))
                                .map(kv -> convertToRow(kv, mode))
                                .collect(Collectors.toList()));
                latestSnapshot = findLatestSnapshot(tableName).id();
            }
        }
    }

    private void compactAndCheck(
            String tableName,
            String compactQuery,
            String selectQuery,
            long latestSnapshot,
            List<Row> expectedData)
            throws IOException {
        batchSql(compactQuery);
        Snapshot snapshot = findLatestSnapshot(tableName);
        assertThat(snapshot.id()).isEqualTo(latestSnapshot + 1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        // check idempotence
        batchSql(compactQuery);
        assertThat(findLatestSnapshot(tableName).id()).isEqualTo(snapshot.id());

        // read data
        List<Row> readData = batchSql(selectQuery);
        assertThat(readData).containsExactlyInAnyOrderElementsOf(expectedData);
    }

    private boolean partFilter(
            KeyValue kv, BinaryRowData partition, TestKeyValueGenerator.GeneratorMode mode) {
        RowData record = kv.value();
        if (mode == SINGLE_PARTITIONED) {
            return record.getString(0).equals(partition.getString(0));
        } else if (mode == MULTI_PARTITIONED) {
            return record.getString(0).equals(partition.getString(0))
                    && record.getInt(1) == partition.getInt(1);
        }
        return true;
    }

    private String partAsString(Map<String, String> partSpec, boolean predicate) {
        String dt = String.format("dt = '%s'", partSpec.get("dt"));
        String hr = partSpec.get("hr");
        if (hr == null) {
            return dt;
        }
        hr = String.format("hr = %s", hr);
        return predicate ? String.join(" AND ", dt, hr) : String.join(", ", dt, hr);
    }

    private BinaryRowData pickPartition(List<BinaryRowData> partitions) {
        Random random = new Random();
        int idx = random.nextInt(partitions.size());
        return partitions.remove(idx);
    }

    private List<KeyValue> generateData(int numRecords) {
        List<KeyValue> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            KeyValue kv = generator.next();
            if (kv.valueKind() == ValueKind.ADD) {
                data.add(kv);
            }
        }
        return data;
    }

    private Row convertToRow(KeyValue keyValue, TestKeyValueGenerator.GeneratorMode mode) {
        byte kind = keyValue.valueKind().toByteValue();
        RowData record = keyValue.value();
        String rowKind = RowKind.fromByteValue(kind == 0 ? kind : 3).shortString();
        if (mode == NON_PARTITIONED) {
            return changelogRow(rowKind, record.getInt(0), record.getLong(1), record.getLong(2));
        } else if (mode == SINGLE_PARTITIONED) {
            return changelogRow(
                    rowKind,
                    record.getString(0).toString(),
                    record.getInt(1),
                    record.getLong(2),
                    record.getLong(3));
        }
        return changelogRow(
                rowKind,
                record.getString(0).toString(),
                record.getInt(1),
                record.getInt(2),
                record.getLong(3),
                record.getLong(4));
    }

    private String kvAsString(KeyValue keyValue, TestKeyValueGenerator.GeneratorMode mode) {
        RowData record = keyValue.value();
        switch (mode) {
            case NON_PARTITIONED:
                return String.format(
                        "(%d, %d, %d)", record.getInt(0), record.getLong(1), record.getLong(2));
            case SINGLE_PARTITIONED:
                return String.format(
                        "('%s', %d, %d, %d)",
                        record.getString(0),
                        record.getInt(1),
                        record.getLong(2),
                        record.getLong(3));
            case MULTI_PARTITIONED:
                return String.format(
                        "('%s', %d, %d, %d, %d)",
                        record.getString(0),
                        record.getInt(1),
                        record.getInt(2),
                        record.getLong(3),
                        record.getLong(4));
            default:
                throw new UnsupportedOperationException("unsupported mode");
        }
    }

    private String getSnapshotDir(String tableName) {
        return path
                + relativeTablePath(
                        ObjectIdentifier.of(
                                bEnv.getCurrentCatalog(), bEnv.getCurrentDatabase(), tableName))
                + "/snapshot";
    }

    private Snapshot findLatestSnapshot(String tableName) throws IOException {
        String snapshotDir = getSnapshotDir(tableName);
        Long latest = SnapshotFinder.findLatest(new Path(URI.create(snapshotDir)));
        return Snapshot.fromPath(
                new Path(URI.create(snapshotDir + String.format("/snapshot-%d", latest))));
    }
}
