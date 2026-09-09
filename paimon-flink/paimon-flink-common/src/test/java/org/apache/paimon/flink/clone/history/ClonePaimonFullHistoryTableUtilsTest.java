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

package org.apache.paimon.flink.clone.history;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ClonePaimonFullHistoryTableUtils}. */
public class ClonePaimonFullHistoryTableUtilsTest {

    @Test
    public void testValidateSingleTableClone() {
        assertThatNoException()
                .isThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        null,
                                        null,
                                        null,
                                        null,
                                        false));
    }

    @Test
    public void testAllowMissingTargetIdentifier() {
        assertThatNoException()
                .isThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        false));
    }

    @Test
    public void testRejectFilteredClone() {
        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        "dt = '2026-07-02'",
                                        null,
                                        null,
                                        null,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("where");
    }

    @Test
    public void testRejectMetaOnlyClone() {
        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        null,
                                        null,
                                        null,
                                        null,
                                        true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("meta_only");
    }

    @Test
    public void testRejectCatalogOrDatabaseClone() {
        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        null, null, null, null, null, null, null, null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("single table");

        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        null,
                                        "target_db",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("single table");
    }

    @Test
    public void testRejectIncludedOrExcludedTables() {
        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        null,
                                        Collections.singletonList("db.tbl"),
                                        null,
                                        null,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("included_tables");

        assertThatThrownBy(
                        () ->
                                ClonePaimonFullHistoryTableUtils.validateFullHistoryOptions(
                                        "default",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        null,
                                        null,
                                        Collections.singletonList("db.tbl"),
                                        null,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("excluded_tables");
    }
}
