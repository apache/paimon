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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.types.RowKind.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

class LookupMergeFunctionTest {

    @Test
    public void testKeepLowestHighLevel() {
        LookupMergeFunction function =
                (LookupMergeFunction)
                        LookupMergeFunction.wrap(
                                        DeduplicateMergeFunction.factory(), null, null, null)
                                .create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(2));
        KeyValue kv = function.getResult();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(2);
    }

    @Test
    public void testLevelNegative() {
        LookupMergeFunction function =
                (LookupMergeFunction)
                        LookupMergeFunction.wrap(
                                        DeduplicateMergeFunction.factory(), null, null, null)
                                .create();
        function.reset();
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(2)).setLevel(-1));
        function.add(new KeyValue().replace(row(1), 1, INSERT, row(1)).setLevel(-1));
        KeyValue kv = function.getResult();
        assertThat(kv).isNotNull();
        assertThat(kv.value().getInt(0)).isEqualTo(1);
    }
}
