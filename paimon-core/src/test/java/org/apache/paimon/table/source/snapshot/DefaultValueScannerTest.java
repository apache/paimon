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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** test default value on streaming scan. */
public class DefaultValueScannerTest extends ScannerTestBase {
    @Test
    public void testDefaultValue() throws Exception {
        TableRead read = table.newRead();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        StreamDataTableScan scan = table.newStreamScan();

        write.write(rowData(1, 10, 101L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, null));
        commit.commit(1, write.prepareCommit(true, 1));

        {
            TableScan.Plan plan = scan.plan();
            assertThat(getResult(read, plan.splits()))
                    .hasSameElementsAs(Arrays.asList("+I 1|10|100"));
        }

        write.write(rowData(2, 11, 200L));
        write.write(rowData(2, 12, null));
        commit.commit(1, write.prepareCommit(true, 1));

        {
            // Predicate pushdown for default fields will not work
            PredicateBuilder predicateBuilder =
                    new PredicateBuilder(table.schema().logicalRowType());

            Predicate ptEqual = predicateBuilder.equal(predicateBuilder.indexOf("pt"), 2);
            Predicate bEqual = predicateBuilder.equal(predicateBuilder.indexOf("b"), 200);
            Predicate predicate = PredicateBuilder.and(ptEqual, bEqual);

            TableScan.Plan plan = scan.withFilter(predicate).plan();
            read = table.newRead().withFilter(predicate);
            List<String> result = getResult(read, plan.splits());
            assertThat(result).hasSameElementsAs(Arrays.asList("+I 2|11|200", "+I 2|12|100"));
        }
        write.close();
        commit.close();
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        Options options = new Options();
        options.set(
                String.format(
                        "%s.%s.%s",
                        CoreOptions.FIELDS_PREFIX, "b", CoreOptions.DEFAULT_VALUE_SUFFIX),
                "100");
        return createFileStoreTable(options);
    }
}
