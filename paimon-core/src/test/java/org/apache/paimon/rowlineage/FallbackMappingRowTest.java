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

import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.GenericRow;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Test for {@link FallbackMappingRow}. */
public class FallbackMappingRowTest {

    @Test
    public void testBasic() {
        int[] project = new int[7];

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 111L);
        genericRow.setField(1, 222L);

        project[0] = -1;
        project[1] = -1;
        project[2] = -1;
        project[3] = -1;
        project[4] = -1;
        project[5] = 1;
        project[6] = 0;

        GenericRow mainRow = new GenericRow(7);
        mainRow.setField(0, 0L);
        mainRow.setField(1, 1L);
        mainRow.setField(2, 2L);
        mainRow.setField(3, 3L);
        FallbackMappingRow fallbackMappingRow = new FallbackMappingRow(project);
        fallbackMappingRow.replace(mainRow, genericRow);

        assertThat(fallbackMappingRow.getFieldCount()).isEqualTo(7);
        assertThat(fallbackMappingRow.getLong(6)).isEqualTo(111L);
        assertThat(fallbackMappingRow.getLong(5)).isEqualTo(222L);
        assertThat(fallbackMappingRow.getLong(0)).isEqualTo(0L);
        assertThat(fallbackMappingRow.getLong(1)).isEqualTo(1L);
        assertThat(fallbackMappingRow.getLong(2)).isEqualTo(2L);
        assertThat(fallbackMappingRow.getLong(3)).isEqualTo(3L);
    }
}
