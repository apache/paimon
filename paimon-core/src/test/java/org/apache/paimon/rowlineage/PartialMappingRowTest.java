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

package org.apache.paimon.rowlineage;

import org.apache.paimon.data.GenericRow;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Test for {@link PartialMappingRow}. */
public class PartialMappingRowTest {

    @Test
    public void testBasic() {
        int[] project = new int[2];

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 111L);
        genericRow.setField(1, 222L);

        project[0] = 6;
        project[1] = 5;

        GenericRow mainRow = new GenericRow(7);
        PartialMappingRow partialMappingRow = new PartialMappingRow(project);
        partialMappingRow.replace(mainRow, genericRow);

        assertThat(partialMappingRow.getFieldCount()).isEqualTo(7);
        assertThat(partialMappingRow.getLong(6)).isEqualTo(111L);
        assertThat(partialMappingRow.getLong(5)).isEqualTo(222L);
    }
}
