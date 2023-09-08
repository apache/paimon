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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.DynamicBucketRow;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.CROSS_PARTITION_UPSERT_BOOTSTRAP_MIN_PARTITION;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexBootstrap}. */
public class IndexBootstrapTest extends TableTestBase {

    @Test
    public void testBoostrap() throws Exception {
        Identifier identifier = identifier("T");
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("col", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .primaryKey("pk")
                        .partitionKeys("pt")
                        .options(options.toMap())
                        .build();
        catalog.createTable(identifier, schema, true);
        Table table = catalog.getTable(identifier);

        write(
                table,
                row(1, 1, 1, 2),
                row(1, 2, 2, 3),
                row(1, 3, 3, 4),
                row(2, 4, 4, 5),
                row(2, 5, 5, 6),
                row(3, 6, 6, 7),
                row(3, 7, 7, 8));

        IndexBootstrap indexBootstrap = new IndexBootstrap(table);
        List<GenericRow> result = new ArrayList<>();
        Consumer<InternalRow> consumer =
                row -> result.add(GenericRow.of(row.getInt(0), row.getInt(1), row.getInt(2)));

        // output key and bucket

        indexBootstrap.bootstrap(2, 0, consumer);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(7, 3, 8),
                        GenericRow.of(5, 2, 6),
                        GenericRow.of(1, 1, 2),
                        GenericRow.of(3, 1, 4));
        result.clear();

        indexBootstrap.bootstrap(2, 1, consumer);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(2, 1, 3), GenericRow.of(4, 2, 5), GenericRow.of(6, 3, 7));
        result.clear();

        // test bootstrap min partition
        indexBootstrap =
                new IndexBootstrap(
                        table.copy(
                                Collections.singletonMap(
                                        CROSS_PARTITION_UPSERT_BOOTSTRAP_MIN_PARTITION.key(),
                                        "2")));

        indexBootstrap.bootstrap(2, 0, consumer);
        assertThat(result)
                .containsExactlyInAnyOrder(GenericRow.of(7, 3, 8), GenericRow.of(5, 2, 6));
        result.clear();

        indexBootstrap.bootstrap(2, 1, consumer);
        assertThat(result)
                .containsExactlyInAnyOrder(GenericRow.of(4, 2, 5), GenericRow.of(6, 3, 7));
        result.clear();
    }

    private DynamicBucketRow row(int pt, int col, int pk, int bucket) {
        GenericRow row = GenericRow.of(pt, col, pk);
        return new DynamicBucketRow(row, bucket);
    }
}
