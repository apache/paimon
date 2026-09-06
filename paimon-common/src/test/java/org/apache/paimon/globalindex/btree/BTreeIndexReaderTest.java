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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BTreeIndexReader} to read a single file. */
@ExtendWith(ParameterizedTestExtension.class)
public class BTreeIndexReaderTest extends AbstractIndexReaderTest {

    public BTreeIndexReaderTest(List<Object> args) {
        super(args);
    }

    @Override
    protected GlobalIndexReader prepareDataAndCreateReader() throws Exception {
        GlobalIndexIOMeta written = writeData(data);
        return globalIndexer.createReader(
                fileReader,
                Collections.singletonList(written),
                dataNum,
                newDirectExecutorService());
    }

    @TestTemplate
    public void testDescendingTopN() throws Exception {
        int limit = 20;
        FieldRef ref = new FieldRef(1, "testField", dataType);
        Object[] valuesByRowId = valuesByRowId();

        try (GlobalIndexReader reader = prepareDataAndCreateReader()) {
            GlobalIndexResult result =
                    reader.visitTopN(new TopN(ref, DESCENDING, NULLS_LAST, limit)).join().get();
            assertThat(result.results().getLongCardinality()).isEqualTo(limit);

            Object boundary = data.get(dataNum - limit).getKey();
            for (long rowId : result.results()) {
                assertThat(comparator.compare(valuesByRowId[(int) rowId], boundary))
                        .isGreaterThanOrEqualTo(0);
            }

            GlobalIndexResult ascending =
                    reader.visitTopN(new TopN(ref, ASCENDING, NULLS_LAST, limit)).join().get();
            assertThat(ascending.results().getLongCardinality()).isEqualTo(limit);
            boundary = data.get(limit - 1).getKey();
            for (long rowId : ascending.results()) {
                assertThat(comparator.compare(valuesByRowId[(int) rowId], boundary))
                        .isLessThanOrEqualTo(0);
            }

            assertThat(
                            reader.visitTopN(new TopN(ref, DESCENDING, NULLS_LAST, 0))
                                    .join()
                                    .get()
                                    .results())
                    .isEmpty();
        }

        int nullCount = dataNum / 10;
        for (int i = dataNum - nullCount; i < dataNum; i++) {
            data.get(i).setLeft(null);
        }
        valuesByRowId = valuesByRowId();
        try (GlobalIndexReader reader = prepareDataAndCreateReader()) {
            GlobalIndexResult nullsFirst =
                    reader.visitTopN(new TopN(ref, DESCENDING, NULLS_FIRST, limit)).join().get();
            assertThat(nullsFirst.results().getLongCardinality()).isEqualTo(limit);
            for (long rowId : nullsFirst.results()) {
                assertThat(valuesByRowId[(int) rowId]).isNull();
            }

            GlobalIndexResult nullsLast =
                    reader.visitTopN(new TopN(ref, DESCENDING, NULLS_LAST, limit)).join().get();
            assertThat(nullsLast.results().getLongCardinality()).isEqualTo(limit);
            Object boundary = data.get(dataNum - nullCount - limit).getKey();
            for (long rowId : nullsLast.results()) {
                Object value = valuesByRowId[(int) rowId];
                assertThat(value).isNotNull();
                assertThat(comparator.compare(value, boundary)).isGreaterThanOrEqualTo(0);
            }

            GlobalIndexResult ascendingNullsFirst =
                    reader.visitTopN(new TopN(ref, ASCENDING, NULLS_FIRST, limit)).join().get();
            assertThat(ascendingNullsFirst.results().getLongCardinality()).isEqualTo(limit);
            for (long rowId : ascendingNullsFirst.results()) {
                assertThat(valuesByRowId[(int) rowId]).isNull();
            }

            GlobalIndexResult ascendingNullsLast =
                    reader.visitTopN(new TopN(ref, ASCENDING, NULLS_LAST, limit)).join().get();
            assertThat(ascendingNullsLast.results().getLongCardinality()).isEqualTo(limit);
            boundary = data.get(limit - 1).getKey();
            for (long rowId : ascendingNullsLast.results()) {
                Object value = valuesByRowId[(int) rowId];
                assertThat(value).isNotNull();
                assertThat(comparator.compare(value, boundary)).isLessThanOrEqualTo(0);
            }
        }
    }

    @TestTemplate
    public void testTopNOnlyDeserializesRemainingRowIds() {
        MemorySliceOutput output = new MemorySliceOutput(16);
        output.writeVarLenInt(3);
        output.writeVarLenLong(10);
        output.writeVarLenLong(20);

        assertThat(BTreeIndexReader.deserializeRowIds(output.toSlice(), 2))
                .containsExactly(10L, 20L);
    }

    private Object[] valuesByRowId() {
        Object[] values = new Object[dataNum];
        data.forEach(pair -> values[pair.getValue().intValue()] = pair.getKey());
        return values;
    }
}
