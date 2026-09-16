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

package org.apache.paimon.operation.commit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestEntryChanges}. */
public class ManifestEntryChangesTest {

    @Test
    public void testCollectPreservesIndexSchemaId() {
        IndexFileMeta indexFile =
                new IndexFileMeta(
                        "btree",
                        "index-file",
                        1L,
                        1L,
                        null,
                        null,
                        new GlobalIndexMeta(0L, 0L, 0, null, null),
                        11L);
        CommitMessageImpl message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        null,
                        DataIncrement.indexIncrement(Collections.singletonList(indexFile)),
                        CompactIncrement.emptyIncrement());

        ManifestEntryChanges changes = new ManifestEntryChanges(1);
        changes.collect(message);

        assertThat(changes.appendIndexFiles)
                .singleElement()
                .satisfies(
                        entry -> {
                            assertThat(entry.schemaId()).isEqualTo(11L);
                            assertThat(entry.indexFile().schemaId()).isEqualTo(11L);
                        });
    }
}
