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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowPartitionKeyExtractor}. */
class RowPartitionKeyExtractorTest {

    @Test
    void testOnlyDeleteDeletesAKey() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(1, "f1", DataTypes.INT()),
                        new DataField(2, "f2", DataTypes.INT()));
        TableSchema schema =
                new TableSchema(
                        1,
                        fields,
                        2,
                        Collections.singletonList("f0"),
                        Collections.singletonList("f1"),
                        new HashMap<>(),
                        "");
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);

        assertThat(extractor.isDelete(GenericRow.ofKind(RowKind.DELETE, 1, 2, 3))).isTrue();
        // +U and +I write the key, and -U is only the first half of an update
        assertThat(extractor.isDelete(GenericRow.ofKind(RowKind.INSERT, 1, 2, 3))).isFalse();
        assertThat(extractor.isDelete(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 2, 3))).isFalse();
        assertThat(extractor.isDelete(GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 3)))
                .isFalse();
    }
}
