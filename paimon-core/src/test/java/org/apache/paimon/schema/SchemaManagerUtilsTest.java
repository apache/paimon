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

package org.apache.paimon.schema;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for option rewriting in {@link SchemaManagerUtils}. */
public class SchemaManagerUtilsTest {

    private static Map<String, String> options(String... pairs) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            options.put(pairs[i], pairs[i + 1]);
        }
        return options;
    }

    @Test
    public void testNestedRenameLeavesRootOptionKeysAlone() {
        Map<String, String> options =
                options("fields.v.map.storage-layout", "shared-shredding", "bucket-key", "a");

        Map<String, String> rewritten =
                SchemaManagerUtils.applyRenameColumnsToOptions(
                        options,
                        Collections.singletonList(
                                SchemaChange.renameColumn(
                                        new String[] {"v", "value", "f1"}, "f100")));

        // a nested rename must not relocate the root column's options to the new nested name
        assertThat(rewritten).containsEntry("fields.v.map.storage-layout", "shared-shredding");
        assertThat(rewritten).doesNotContainKey("fields.f100.map.storage-layout");
    }

    @Test
    public void testTwoNestedRenamesUnderOneRootDoNotCrash() {
        Map<String, String> options = options("fields.v.aggregate-function", "last_non_null");

        Map<String, String> rewritten =
                SchemaManagerUtils.applyRenameColumnsToOptions(
                        options,
                        Arrays.asList(
                                SchemaChange.renameColumn(new String[] {"v", "value", "f1"}, "f1n"),
                                SchemaChange.renameColumn(
                                        new String[] {"v", "value", "f2"}, "f2n")));

        assertThat(rewritten).containsEntry("fields.v.aggregate-function", "last_non_null");
    }

    @Test
    public void testSpacedCsvOptionEntriesMatchRename() {
        Map<String, String> options =
                options(
                        "bucket-key", "a, b",
                        "sequence.field", "a, b",
                        "clustering.columns", "b");

        Map<String, String> rewritten =
                SchemaManagerUtils.applyRenameColumnsToOptions(
                        options, Collections.singletonList(SchemaChange.renameColumn("b", "c")));

        // canonical readers trim the entries, so the rewrite must match them trimmed
        assertThat(rewritten).containsEntry("bucket-key", "a,c");
        assertThat(rewritten).containsEntry("sequence.field", "a,c");
        assertThat(rewritten).containsEntry("clustering.columns", "c");
    }

    @Test
    public void testClusteringColumnsFollowRename() {
        Map<String, String> options = options("clustering.columns", "c,d");

        Map<String, String> rewritten =
                SchemaManagerUtils.applyRenameColumnsToOptions(
                        options, Collections.singletonList(SchemaChange.renameColumn("c", "c2")));

        assertThat(rewritten).containsEntry("clustering.columns", "c2,d");
    }

    @Test
    public void testSequenceGroupValueEntriesMatchRenameTrimmed() {
        Map<String, String> options = options("fields.x.sequence-group", "a, b");

        Map<String, String> rewritten =
                SchemaManagerUtils.applyRenameColumnsToOptions(
                        options, Collections.singletonList(SchemaChange.renameColumn("b", "c")));

        assertThat(rewritten).containsEntry("fields.x.sequence-group", "a,c");
    }
}
