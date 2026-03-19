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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bucket filtering with compound predicates on a partitioned, fixed-bucket, append-only
 * table.
 */
public class BucketFilterScanTest extends TableTestBase {

    /**
     * Tests bucket filtering with compound predicates on a single-field bucket key.
     *
     * <p>Table schema:
     *
     * <ul>
     *   <li>Partition key: column 'a' (INT)
     *   <li>Bucket key: column 'b' (INT)
     *   <li>Bucket count: 10
     * </ul>
     *
     * <p>Data distribution: 5 partitions (a=1 to 5) × 20 b-values (b=1 to 20) = 100 rows.
     *
     * <p>Test scenarios:
     *
     * <ol>
     *   <li>Predicate: (a &lt; 3 AND b = 5) OR (a = 3 AND b = 7) - Tests partition range filter
     *       with bucket equality, combined with OR. Expected: buckets for partition 1,2 with b=5
     *       and partition 3 with b=7.
     *   <li>Predicate: (a &lt; 3 AND b = 5) OR (a = 3 AND b &lt; 100) - Tests partition range with
     *       bucket equality, OR partition equality with bucket range. Expected: mixed buckets from
     *       partition 3 and specific buckets from partitions 1,2.
     *   <li>Predicate: (a = 2 AND b = 5) OR (a = 3 AND b = 7) - Tests partition equality with
     *       bucket equality in both OR branches. Expected: exact bucket matching for each
     *       partition-b combination.
     * </ol>
     */
    @Test
    public void testBucketFilterWithCompoundPredicate() throws Exception {
        // ---- schema & table ----
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .option(CoreOptions.BUCKET.key(), "10")
                        .option(CoreOptions.BUCKET_KEY.key(), "b")
                        .build();

        Identifier tableId = identifier("test_bucket_filter");
        catalog.createTable(tableId, schema, false);
        Table table = catalog.getTable(tableId);

        // ---- write data: 5 partitions × 20 b-values = 100 rows ----
        GenericRow[] rows = new GenericRow[100];
        int idx = 0;
        for (int a = 1; a <= 5; a++) {
            for (int b = 1; b <= 20; b++) {
                rows[idx++] = GenericRow.of(a, b, a * 100 + b);
            }
        }
        write(table, rows);
        PredicateBuilder pb = new PredicateBuilder(table.rowType());

        // ---- build predicate: (a < 3 AND b = 5) OR (a = 3 AND b = 7) ----
        Predicate predicate1 =
                PredicateBuilder.or(
                        PredicateBuilder.and(pb.lessThan(0, 3), pb.equal(1, 5)),
                        PredicateBuilder.and(pb.equal(0, 3), pb.equal(1, 7)));
        assertThat(plan(table, predicate1)).containsExactlyInAnyOrder("3,1", "1,6", "2,6");

        // ---- build predicate: (a < 3 AND b = 5) OR (a = 3 AND b < 100) ----
        Predicate predicate2 =
                PredicateBuilder.or(
                        PredicateBuilder.and(pb.lessThan(0, 3), pb.equal(1, 5)),
                        PredicateBuilder.and(pb.equal(0, 3), pb.lessThan(1, 100)));
        assertThat(plan(table, predicate2))
                .containsExactlyInAnyOrder(
                        "3,0", "3,1", "1,6", "3,4", "3,5", "2,6", "3,6", "3,7", "3,8");

        // ---- build predicate: (a = 2 AND b = 5) OR (a = 3 AND b = 7) ----
        Predicate predicate3 =
                PredicateBuilder.or(
                        PredicateBuilder.and(pb.equal(0, 2), pb.equal(1, 5)),
                        PredicateBuilder.and(pb.equal(0, 3), pb.equal(1, 7)));
        assertThat(plan(table, predicate3)).containsExactlyInAnyOrder("3,1", "2,6");
    }

    /**
     * Tests bucket filtering with compound predicates on a composite (multi-field) bucket key.
     *
     * <p>Table schema:
     *
     * <ul>
     *   <li>Partition key: column 'a' (INT)
     *   <li>Bucket key: columns 'b' and 'c' (composite, INT)
     *   <li>Bucket count: 10
     * </ul>
     *
     * <p>Data distribution: 5 partitions (a=1 to 5) × 20 b-values (b=1 to 20) × 10 c-values (c=0 to
     * 9) = 1000 rows.
     *
     * <p>Test scenarios:
     *
     * <ol>
     *   <li>Predicate: ((a &lt; 3 AND b = 5) OR (a = 3 AND b = 7)) AND c = 5 - Tests nested OR
     *       within AND, with partition range, bucket field equality, and additional bucket field
     *       filter. The 'c = 5' condition is part of the composite bucket key, affecting bucket
     *       selection.
     *   <li>Predicate: ((a &lt; 3 AND b = 5) OR (a = 3 AND b &lt; 100)) AND c = 5 - Tests range
     *       predicate on one bucket field (b) combined with equality on another (c). Validates
     *       handling of multiple bucket key fields with different predicate types.
     *   <li>Predicate: ((a = 2 AND b = 5) OR (a = 3 AND b = 7)) AND c = 5 - Tests exact matching on
     *       both partition and bucket fields. The composite bucket key (b,c) ensures precise bucket
     *       targeting.
     * </ol>
     */
    @Test
    public void testCompositeBucketFilterWithCompoundPredicate() throws Exception {
        // ---- schema & table ----
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .option(CoreOptions.BUCKET.key(), "10")
                        .option(CoreOptions.BUCKET_KEY.key(), "b,c")
                        .build();

        Identifier tableId = identifier("test_composite_bucket_filter");
        catalog.createTable(tableId, schema, false);
        Table table = catalog.getTable(tableId);

        // ---- write data: 5 partitions × 20 b-values x 10 c-values = 1000 rows ----
        GenericRow[] rows = new GenericRow[1000];
        int idx = 0;
        for (int a = 1; a <= 5; a++) {
            for (int b = 1; b <= 20; b++) {
                for (int c = 0; c < 10; c++) {
                    rows[idx++] = GenericRow.of(a, b, c);
                }
            }
        }
        write(table, rows);
        PredicateBuilder pb = new PredicateBuilder(table.rowType());

        // ---- build predicate: ((a < 3 AND b = 5) OR (a = 3 AND b = 7)) AND c = 5 ----
        Predicate predicate1 =
                PredicateBuilder.and(
                        PredicateBuilder.or(
                                PredicateBuilder.and(pb.lessThan(0, 3), pb.equal(1, 5)),
                                PredicateBuilder.and(pb.equal(0, 3), pb.equal(1, 7))),
                        pb.equal(2, 5));
        assertThat(plan(table, predicate1)).containsExactlyInAnyOrder("1,0", "2,0", "3,5");

        // ---- build predicate: ((a < 3 AND b = 5) OR (a = 3 AND b < 100)) AND c = 5 ----
        Predicate predicate2 =
                PredicateBuilder.and(
                        PredicateBuilder.or(
                                PredicateBuilder.and(pb.lessThan(0, 3), pb.equal(1, 5)),
                                PredicateBuilder.and(pb.equal(0, 3), pb.lessThan(1, 100))),
                        pb.equal(2, 5));
        assertThat(plan(table, predicate2))
                .containsExactlyInAnyOrder(
                        "3,9", "1,0", "2,0", "3,0", "3,1", "3,2", "3,3", "3,4", "3,5", "3,6", "3,7",
                        "3,8");

        // ---- build predicate: ((a = 2 AND b = 5) OR (a = 3 AND b = 7)) AND c = 5 ----
        Predicate predicate3 =
                PredicateBuilder.and(
                        PredicateBuilder.or(
                                PredicateBuilder.and(pb.equal(0, 2), pb.equal(1, 5)),
                                PredicateBuilder.and(pb.equal(0, 3), pb.equal(1, 7))),
                        pb.equal(2, 5));
        assertThat(plan(table, predicate3)).containsExactlyInAnyOrder("2,0", "3,5");
    }

    private Set<String> plan(Table table, Predicate predicate) {
        return table.newReadBuilder().withFilter(predicate).newScan().plan().splits().stream()
                .map(
                        split -> {
                            DataSplit dataSplit = (DataSplit) split;
                            int partitionA = dataSplit.partition().getInt(0);
                            int bucket = dataSplit.bucket();
                            return partitionA + "," + bucket;
                        })
                .collect(Collectors.toSet());
    }
}
