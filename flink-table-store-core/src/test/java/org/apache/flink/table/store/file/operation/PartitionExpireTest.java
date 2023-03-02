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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.table.AbstractFileStoreTable;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.StreamTableWrite;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.store.CoreOptions.PARTITION_EXPIRATION_TIME;
import static org.apache.flink.table.store.CoreOptions.WRITE_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionExpire}. */
public class PartitionExpireTest {

    @TempDir java.nio.file.Path tempDir;

    private Path path;
    private FileStoreTable table;

    @BeforeEach
    public void beforeEach() {
        path = new Path(tempDir.toUri());
    }

    @Test
    public void testNonPartitionedTable() {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        assertThatThrownBy(
                        () ->
                                schemaManager.createTable(
                                        new Schema(
                                                RowType.of(VarCharType.STRING_TYPE).getFields(),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.singletonMap(
                                                        PARTITION_EXPIRATION_TIME.key(), "1 d"),
                                                "")))
                .hasMessageContaining(
                        "Can not set 'partition.expiration-time' for non-partitioned table");
    }

    @Test
    public void test() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        schemaManager.createTable(
                new Schema(
                        RowType.of(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE).getFields(),
                        Collections.singletonList("f0"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        ""));
        table = FileStoreTableFactory.create(LocalFileIO.create(), path);

        write("20230101", "11");
        write("20230101", "12");
        write("20230103", "31");
        write("20230103", "32");
        write("20230105", "51");

        PartitionExpire expire = newExpire();
        expire.setLastCheck(date(1));

        expire.expire(date(3));
        assertThat(read())
                .containsExactlyInAnyOrder(
                        "20230101:11", "20230101:12", "20230103:31", "20230103:32", "20230105:51");

        expire.expire(date(5));
        assertThat(read()).containsExactlyInAnyOrder("20230103:31", "20230103:32", "20230105:51");

        // PARTITION_EXPIRATION_INTERVAL not trigger
        expire.expire(date(6));
        assertThat(read()).containsExactlyInAnyOrder("20230103:31", "20230103:32", "20230105:51");

        expire.expire(date(8));
        assertThat(read()).isEmpty();
    }

    private List<String> read() throws IOException {
        List<String> ret = new ArrayList<>();
        table.newRead()
                .createReader(table.newScan().plan().splits())
                .forEachRemaining(row -> ret.add(row.getString(0) + ":" + row.getString(1)));
        return ret;
    }

    private LocalDateTime date(int day) {
        return LocalDateTime.of(LocalDate.of(2023, 1, day), LocalTime.MIN);
    }

    private void write(String f0, String f1) throws Exception {
        StreamTableWrite write =
                table.copy(Collections.singletonMap(WRITE_ONLY.key(), "true")).newWrite("");
        write.write(GenericRow.of(BinaryString.fromString(f0), BinaryString.fromString(f1)));
        table.newCommit("").commit(0, write.prepareCommit(true, 0));
        write.close();
    }

    private PartitionExpire newExpire() {
        Map<String, String> options = new HashMap<>();
        options.put(PARTITION_EXPIRATION_TIME.key(), "2 d");
        options.put(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "1 d");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        return ((AbstractFileStoreTable) table.copy(options)).store().newPartitionExpire("");
    }
}
