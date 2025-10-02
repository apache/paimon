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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.table.system.AllPartitionsTable.ALL_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AllPartitionsTable}. */
public class AllPartitionsTableTest extends TableTestBase {

    private AllPartitionsTable allPartitionsTable;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .partitionKeys("f1")
                        .build();
        catalog.createTable(identifier, schema, true);

        write(getTable(identifier), GenericRow.of(1, 1, 1));
        allPartitionsTable =
                (AllPartitionsTable)
                        catalog.getTable(new Identifier(SYSTEM_DATABASE_NAME, ALL_PARTITIONS));
    }

    @Test
    public void testAllPartitionsTable() throws Exception {
        List<String> result =
                read(allPartitionsTable).stream()
                        .map(Objects::toString)
                        .collect(Collectors.toList());
        result = result.stream().filter(r -> !r.contains("path")).collect(Collectors.toList());
        assertThat(result.get(0).toString()).startsWith("+I(default,T,f1=1,1,680,1,");
    }
}
