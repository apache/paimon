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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartitionBucketMapping}. */
public class PartitionBucketMappingTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()
                    },
                    new String[] {"pt", "k", "v"});

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testDefaultBucketCount() {
        PartitionBucketMapping mapping = new PartitionBucketMapping(16);

        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(16);
        assertThat(mapping.resolveNumBuckets(partition(42))).isEqualTo(16);
    }

    @Test
    public void testExplicitPartitionMapping() {
        Map<BinaryRow, Integer> partitionMap = new HashMap<>();
        partitionMap.put(partition(1), 32);
        partitionMap.put(partition(2), 64);

        PartitionBucketMapping mapping = new PartitionBucketMapping(16, partitionMap);

        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(32);
        assertThat(mapping.resolveNumBuckets(partition(2))).isEqualTo(64);
        assertThat(mapping.resolveNumBuckets(partition(3))).isEqualTo(16);
    }

    @Test
    public void testLazyLoadResolvesPartitions() throws Exception {
        FileStoreTable table = createPartitionedTable(2);
        writeData(table, GenericRow.of(1, 1, 100L), GenericRow.of(2, 2, 200L));

        PartitionBucketMapping mapping = PartitionBucketMapping.lazyLoadFromTable(table);

        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(2);
        assertThat(mapping.resolveNumBuckets(partition(2))).isEqualTo(2);
        assertThat(mapping.resolveNumBuckets(partition(99))).isEqualTo(2);
    }

    @Test
    public void testLazyLoadFromNonPartitionedTable() throws Exception {
        FileStoreTable table = createNonPartitionedTable(4);

        PartitionBucketMapping mapping = PartitionBucketMapping.lazyLoadFromTable(table);

        assertThat(mapping.resolveNumBuckets(BinaryRow.EMPTY_ROW)).isEqualTo(4);
        assertThat(mapping.resolveNumBuckets(partition(1))).isEqualTo(4);
    }

    @Test
    public void testLazyLoadDiscoversNewPartitions() throws Exception {
        FileStoreTable table = createPartitionedTable(2);
        writeData(table, GenericRow.of(1, 1, 100L));

        PartitionBucketMapping mapping = PartitionBucketMapping.lazyLoadFromTable(table);

        // Partition 2 doesn't exist yet — falls back to default
        assertThat(mapping.resolveNumBuckets(partition(2))).isEqualTo(2);

        // Write to partition 2
        writeData(table, GenericRow.of(2, 2, 200L));

        // Same mapping discovers partition 2 because absent results are not cached
        assertThat(mapping.resolveNumBuckets(partition(2))).isEqualTo(2);
    }

    // ---- helpers ----

    private static BinaryRow partition(int value) {
        return BinaryRow.singleColumn(value);
    }

    private FileStoreTable createPartitionedTable(int numBuckets) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, numBuckets);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private FileStoreTable createNonPartitionedTable(int numBuckets) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, numBuckets);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Arrays.asList("k"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private void writeData(FileStoreTable table, GenericRow... rows) throws Exception {
        TableWriteImpl<?> write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (GenericRow row : rows) {
            write.write(row);
        }
        commit.commit(0, write.prepareCommit(false, 0));
        write.close();
        commit.close();
    }
}
