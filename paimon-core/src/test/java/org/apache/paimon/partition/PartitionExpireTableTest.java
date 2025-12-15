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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE;
import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_STRATEGY;
import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_TIME;
import static org.apache.paimon.partition.CustomPartitionExpirationFactory.TABLE_EXPIRE_PARTITIONS;
import static org.assertj.core.api.Assertions.assertThat;

class PartitionExpireTableTest extends TableTestBase {

    @Test
    public void testCustomExpire() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("pt", DataTypes.INT());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(PARTITION_EXPIRATION_STRATEGY.key(), "custom");
        schemaBuilder.option(PARTITION_EXPIRATION_TIME.key(), "1 d");
        schemaBuilder.option(END_INPUT_CHECK_PARTITION_EXPIRE.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        Table table = catalog.getTable(identifier());
        String path = table.options().get("path");
        PartitionEntry expire = new PartitionEntry(BinaryRow.singleColumn(1), 1, 1, 1, 1);
        TABLE_EXPIRE_PARTITIONS.put(path, Collections.singletonList(expire));
        write(table, GenericRow.of(1, 1));
        write(table, GenericRow.of(2, 2));
        assertThat(read(table)).containsExactlyInAnyOrder(GenericRow.of(2, 2));

        try {
            write(table, GenericRow.of(3, 3));
            assertThat(read(table))
                    .containsExactlyInAnyOrder(GenericRow.of(3, 3), GenericRow.of(2, 2));
        } finally {
            TABLE_EXPIRE_PARTITIONS.remove(path);
        }
    }
}
