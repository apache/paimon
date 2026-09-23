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

package org.apache.paimon;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parsing an option value, an enum name or a protocol token uppercases or lowercases it first.
 * Under a Turkish default locale 'i' maps to a dotted capital and 'I' to a dotless small letter, so
 * those conversions must pin {@link Locale#ROOT} or the token no longer matches what it is compared
 * against.
 */
class TurkishLocaleParsingTest {

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
    void partitionMarkDoneActionsParse() {
        // SUCCESS_FILE and DONE_PARTITION both contain an 'i': a locale-sensitive uppercase
        // turns them into names no enum constant has, and valueOf throws
        Options options = new Options();
        options.set(PARTITION_MARK_DONE_ACTION, "success-file,done-partition");

        assertThat(new CoreOptions(options).partitionMarkDoneActions())
                .containsExactlyInAnyOrder(
                        CoreOptions.PartitionMarkDoneAction.SUCCESS_FILE,
                        CoreOptions.PartitionMarkDoneAction.DONE_PARTITION);
    }

    @Test
    void rowKindFromLowerCaseShortString() {
        // "+i" is the only short string this can catch: Turkish differs from ROOT on 'i' and
        // 'I' alone, so "-d" or "-u" would pass whichever conversion the code uses
        assertThat(RowKind.fromShortString("+i")).isEqualTo(RowKind.INSERT);
    }
}
