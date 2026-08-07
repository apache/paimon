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

package org.apache.paimon.flink.action;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MergeIntoActionFactory#create}. */
public class MergeIntoActionFactoryTest extends ActionITCaseBase {

    @BeforeEach
    public void setUp() throws Exception {
        // merge_into loads the target table before parsing --merge_actions, so it must exist and
        // must have primary keys
        DataType[] fieldTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};
        RowType rowType = RowType.of(fieldTypes, new String[] {"k", "v"});
        createFileStoreTable(
                rowType,
                Collections.emptyList(),
                Collections.singletonList("k"),
                Collections.emptyList(),
                new HashMap<>());
    }

    @Test
    public void testMissingMergeActionsReportsRequiredArgument() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        MergeIntoAction.class,
                                        "merge_into",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--source_table",
                                        "S",
                                        "--on",
                                        "T.k = S.k"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Argument 'merge_actions' is required");
    }

    @Test
    public void testCreateWithMergeActions() {
        assertThatCode(
                        () ->
                                assertThat(
                                                createAction(
                                                        MergeIntoAction.class,
                                                        "merge_into",
                                                        "--warehouse",
                                                        warehouse,
                                                        "--database",
                                                        database,
                                                        "--table",
                                                        tableName,
                                                        "--source_table",
                                                        "S",
                                                        "--on",
                                                        "T.k = S.k",
                                                        "--merge_actions",
                                                        "matched-upsert",
                                                        "--matched_upsert_set",
                                                        "v = S.v"))
                                        .isNotNull())
                .doesNotThrowAnyException();
    }
}
