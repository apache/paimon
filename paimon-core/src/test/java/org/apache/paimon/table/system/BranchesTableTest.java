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

package org.apache.paimon.table.system;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.TableCommitApi;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BranchesTable}. */
class BranchesTableTest extends TableTestBase {
    private static final String tableName = "MyTable";
    private FileStoreTable table;
    private BranchesTable branchesTable;

    @BeforeEach
    void before() throws Exception {
        Identifier identifier = identifier(tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .option("tag.automatic-creation", "watermark")
                        .option("tag.creation-period", "daily")
                        .option("tag.num-retained-max", "3")
                        .build();
        catalog.createTable(identifier, schema, true);
        table = (FileStoreTable) catalog.getTable(identifier);
        TableCommitApi commit =
                table.newCommit(commitUser).ignoreEmptyCommit(false).asTableCommitApi();
        commit.commit(
                new ManifestCommittable(
                        0,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2023-07-18T12:00:01"))
                                .getMillisecond()));
        commit.commit(
                new ManifestCommittable(
                        1,
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2023-07-19T12:00:01"))
                                .getMillisecond()));
        branchesTable = (BranchesTable) catalog.getTable(identifier(tableName + "$branches"));
    }

    @Test
    void testEmptyBranches() throws Exception {
        assertThat(read(branchesTable)).isEmpty();
    }

    @Test
    void testBranches() throws Exception {
        table.createBranch("my_branch1", "2023-07-17");
        table.createBranch("my_branch2", "2023-07-18");
        table.createBranch("my_branch3", "2023-07-18");
        List<InternalRow> branches = read(branchesTable);
        assertThat(branches.size()).isEqualTo(3);
        assertThat(
                        branches.stream()
                                .map(v -> v.getString(0).toString())
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("my_branch1", "my_branch2", "my_branch3");
    }
}
