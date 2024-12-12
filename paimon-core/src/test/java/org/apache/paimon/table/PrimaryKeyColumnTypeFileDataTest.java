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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Column type evolution for file data in changelog with key table. */
public class PrimaryKeyColumnTypeFileDataTest extends ColumnTypeFileDataTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
    }

    @Test
    public void testTableSplitFilterNormalFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(6, 200L, 500L);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(predicate)
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.00|203|204|205|206.0|207.0|208|1970-07-29T00:00|210",
                                    "2|300|301|302.00|303|304|305|306.0|307.0|308|1970-11-06T00:00|310");
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(
                                                    new PredicateBuilder(
                                                                    table.schema().logicalRowType())
                                                            .between(6, 200F, 500F))
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.0|203|204.00|205.0|206.0|207.00|208|209|210",
                                    "2|300|301|302.0|303|304.00|305.0|306.0|307.00|308|309|310",
                                    "2|400|401|402.0|403|404.00|405.0|406.0|407.00|408|409|410");
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas) {
        SchemaManager schemaManager = new TestingSchemaManager(tablePath, tableSchemas);
        return new PrimaryKeyFileStoreTable(
                fileIO, tablePathProvider, schemaManager.latest().get()) {
            @Override
            public SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }
}
