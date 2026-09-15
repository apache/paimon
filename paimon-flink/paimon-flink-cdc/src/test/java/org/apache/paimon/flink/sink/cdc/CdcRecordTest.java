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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CdcRecord}. */
class CdcRecordTest {

    @Test
    void fieldNameLowerCaseIndependentOfDefaultLocale() {
        Locale original = Locale.getDefault();
        try {
            // Turkish lowercases 'I' to a dotless glyph under its locale; the record-side
            // keys must match the schema-side Locale.ROOT conversion or the column join
            // silently nulls out
            Locale.setDefault(new Locale("tr", "TR"));
            Map<String, String> data = new HashMap<>();
            data.put("INDEX", "v");
            CdcRecord record = new CdcRecord(RowKind.INSERT, data);

            CdcRecord converted = record.fieldNameLowerCase();

            assertThat(converted.data()).containsEntry("index", "v").doesNotContainKey("ındex");
        } finally {
            Locale.setDefault(original);
        }
    }
}
