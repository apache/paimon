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

package org.apache.paimon.gpu;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests which tables {@link KvLayout} takes and how it places their columns. No device is used. */
public class KvLayoutTest {

    private static final RowType KEY =
            new RowType(
                    Arrays.asList(
                            new DataField(
                                    SpecialFields.KEY_FIELD_ID_START,
                                    SpecialFields.KEY_FIELD_PREFIX + "k",
                                    DataTypes.BIGINT().notNull())));

    private static final RowType VALUE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "k", DataTypes.BIGINT().notNull()),
                            new DataField(1, "qty", DataTypes.INT()),
                            new DataField(2, "price", DataTypes.DOUBLE()),
                            new DataField(3, "ts", DataTypes.BIGINT()),
                            new DataField(4, "day", DataTypes.DATE()),
                            new DataField(5, "comment", DataTypes.STRING())));

    @Test
    public void testPlacesKeysAndSequenceFirst() {
        KvLayout layout = KvLayout.of(KEY, VALUE, options(), new StringBuilder());

        assertThat(layout).isNotNull();
        // _KEY_k, _SEQUENCE_NUMBER as INT64 slots 0 and 1, then _VALUE_KIND as int slot 0.
        assertThat(layout.readColumns).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        assertThat(layout.longSlots).isEqualTo(4);
        assertThat(layout.intSlots).isEqualTo(3);
        assertThat(layout.doubleSlots).isEqualTo(1);
        assertThat(layout.stringColumns).containsExactly(8);
        assertThat(layout.fieldIds.length).isEqualTo(9);
        assertThat(layout.optional[0]).isFalse();
        assertThat(layout.optional[4]).isTrue();
    }

    @ParameterizedTest
    @CsvSource({
        "merge-engine,partial-update,merge-engine",
        "sequence.field,ts,sequence.field",
        "ignore-delete,true,ignore-delete",
        "file.format,orc,parquet",
        "file.compression,lz4,file.compression",
        "data-file.thin-mode,true,thin-mode",
        "file-index.bloom-filter.columns,qty,file-index",
        "sequence.snapshot-ordering,true,sequence.snapshot-ordering"
    })
    public void testRefusesTablesItCannotCompactExactly(String key, String value, String reason) {
        Options options = new Options();
        options.set(key, value);
        StringBuilder why = new StringBuilder();

        assertThat(KvLayout.of(KEY, VALUE, new CoreOptions(options), why)).isNull();
        assertThat(why.toString()).contains(reason);
    }

    @Test
    public void testRefusesUnsupportedColumnTypes() {
        RowType decimals =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.BIGINT().notNull()),
                                new DataField(1, "amount", DataTypes.DECIMAL(10, 2))));
        StringBuilder why = new StringBuilder();

        assertThat(KvLayout.of(KEY, decimals, options(), why)).isNull();
        assertThat(why.toString()).contains("amount");
    }

    private static CoreOptions options() {
        Options options = new Options();
        options.set("file.format", "parquet");
        options.set("file.compression", "zstd");
        return new CoreOptions(options);
    }
}
