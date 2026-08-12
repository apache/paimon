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

package org.apache.paimon.flink;

import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.table.FormatTable;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FormatCatalogTableTest {

    @ParameterizedTest
    @ValueSource(strings = {"field-delimiter", "seq", "delimiter"})
    void testCsvFieldDelimiterFallbackKeys(String fallbackKey) {
        FormatTable table = mock(FormatTable.class);
        when(table.format()).thenReturn(FormatTable.Format.CSV);
        when(table.options()).thenReturn(Collections.singletonMap(fallbackKey, ";"));
        when(table.location()).thenReturn("file:/tmp/t");

        assertThat(new FormatCatalogTable(table).getOptions())
                .containsEntry(CsvOptions.FIELD_DELIMITER.key(), ";");
    }
}
