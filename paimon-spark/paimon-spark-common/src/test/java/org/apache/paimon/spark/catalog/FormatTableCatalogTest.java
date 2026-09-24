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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.table.FormatTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code MOSAIC} contains an 'I', so a provider name folded under a Turkish default locale no
 * longer matches the enum constant it names.
 */
class FormatTableCatalogTest {

    private final FormatTableCatalog catalog = new FormatTableCatalog() {};

    private Locale original;

    @BeforeEach
    void setUp() {
        original = Locale.getDefault();
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterEach
    void tearDown() {
        Locale.setDefault(original);
    }

    @Test
    void everyFormatIsRecognizedInEitherCase() {
        for (FormatTable.Format format : FormatTable.Format.values()) {
            assertThat(catalog.isFormatTable(format.name())).as("%s", format).isTrue();
            assertThat(catalog.isFormatTable(format.name().toLowerCase(Locale.ROOT)))
                    .as("%s lower case", format)
                    .isTrue();
        }
    }

    @Test
    void aNameThatIsNotAFormatIsRejected() {
        assertThat(catalog.isFormatTable(null)).isFalse();
        assertThat(catalog.isFormatTable("")).isFalse();
        assertThat(catalog.isFormatTable("paimon")).isFalse();
        assertThat(catalog.isFormatTable("orcish")).isFalse();
    }
}
