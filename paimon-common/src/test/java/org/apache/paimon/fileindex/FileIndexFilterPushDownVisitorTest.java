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

package org.apache.paimon.fileindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.fileindex.bsi.BitSliceIndexBitmapFileIndexFactory;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileIndexOptions}. */
public class FileIndexFilterPushDownVisitorTest {

    private RowType rowType;
    private FileIndexFilterPushDownVisitor visitor;

    @BeforeEach
    public void setup() {
        this.rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.INT())
                        .field("c", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .build();
        Map<String, String> properties = new HashMap<>();
        properties.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BloomFilterFileIndexFactory.BLOOM_FILTER
                        + "."
                        + CoreOptions.COLUMNS,
                "a,c[hello]");
        properties.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BloomFilterFileIndexFactory.BLOOM_FILTER
                        + "."
                        + "c[hello].fpp",
                "200");
        properties.put(
                FileIndexOptions.FILE_INDEX
                        + "."
                        + BitSliceIndexBitmapFileIndexFactory.BSI_INDEX
                        + "."
                        + CoreOptions.COLUMNS,
                "a,b");
        FileIndexOptions fileIndexOptions = CoreOptions.fromMap(properties).indexColumnsOptions();
        this.visitor = fileIndexOptions.createFilterPushDownPredicateVisitor(rowType);
    }

    @Test
    public void testCreateFilterPushDownPredicateVisitor() {
        Map<String, List<FileIndexFilterPushDownAnalyzer>> analyzers = visitor.getAnalyzers();
        assertThat(analyzers.size()).isEqualTo(3);
        assertThat(analyzers.get("a").size()).isEqualTo(2);
        assertThat(analyzers.get("b").size()).isEqualTo(1);
        assertThat(analyzers.get("c[hello]").size()).isEqualTo(1);
    }

    @Test
    public void testVisitLeafPredicate() {
        // test column a with bloom-filter and bsi index
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).equal(0, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).lessThan(0, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).greaterThan(0, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).startsWith(0, 1)))
                .isFalse();

        // test column b with bsi index
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).equal(1, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).lessThan(1, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).greaterThan(1, 1)))
                .isTrue();
        assertThat(visitor.visit((LeafPredicate) new PredicateBuilder(rowType).startsWith(0, 1)))
                .isFalse();

        // test map column c[hello] with bloom-filter index
        assertThat(
                        visitor.visit(
                                new LeafPredicate(
                                        Equal.INSTANCE,
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                                        2,
                                        "c[hello]",
                                        Collections.singletonList(BinaryString.fromString("a")))))
                .isFalse();
    }
}
