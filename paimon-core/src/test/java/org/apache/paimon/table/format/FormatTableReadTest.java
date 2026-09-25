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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests exact filtering with projected columns in {@link FormatTableRead}. */
class FormatTableReadTest {

    @TempDir Path tempDir;

    @Test
    void testExecuteFilterWithUnprojectedFields() throws Exception {
        RowType type = RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING());
        Files.write(
                tempDir.resolve("data.csv"),
                Arrays.asList("1,1,first", "1,2,second", "2,2,third", "2,3,last"),
                StandardCharsets.UTF_8);
        FormatTable table =
                FormatTable.builder()
                        .fileIO(LocalFileIO.create())
                        .identifier(Identifier.create("test_db", "test_table"))
                        .rowType(type)
                        .partitionKeys(Collections.emptyList())
                        .location(tempDir.toString())
                        .format(FormatTable.Format.CSV)
                        .options(Collections.singletonMap("file.format", "csv"))
                        .build();
        PredicateBuilder predicateBuilder = new PredicateBuilder(type);
        List<Predicate> filters =
                Arrays.asList(
                        predicateBuilder.equal(1, 2),
                        PredicateBuilder.and(
                                predicateBuilder.equal(0, 1), predicateBuilder.equal(1, 2)),
                        PredicateBuilder.or(
                                predicateBuilder.equal(0, 1), predicateBuilder.equal(1, 2)));
        List<List<String>> expected =
                Arrays.asList(
                        Arrays.asList("second:1", "third:2"),
                        Collections.singletonList("second:1"),
                        Arrays.asList("first:1", "second:1", "third:2"));
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        for (int i = 0; i < filters.size(); i++) {
            ReadBuilder builder =
                    table.newReadBuilder()
                            .withReadType(type.project(new int[] {2, 0}))
                            .withFilter(filters.get(i));
            TableRead unfiltered = builder.newRead();
            TableRead filtered = builder.newRead().executeFilter();
            assertThat(readRows(filtered, plan)).containsExactlyElementsOf(expected.get(i));
            assertThat(readRows(filtered, plan)).containsExactlyElementsOf(expected.get(i));
            // Expanding one TableRead must not change another read sharing the builder.
            assertThat(readRows(unfiltered, plan))
                    .containsExactly("first:1", "second:1", "third:2", "last:2");
        }
    }

    private static List<String> readRows(TableRead read, TableScan.Plan plan) throws Exception {
        List<String> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = read.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        assertThat(row.getFieldCount()).isEqualTo(2);
                        result.add(row.getString(0) + ":" + row.getInt(1));
                    });
        }
        return result;
    }
}
