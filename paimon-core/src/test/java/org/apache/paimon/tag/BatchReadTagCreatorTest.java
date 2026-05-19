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

package org.apache.paimon.tag;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BatchReadTagCreator}. */
public class BatchReadTagCreatorTest extends PrimaryKeyTableTestBase {

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        return options;
    }

    @Test
    public void testCreateAndDeleteReadTag() throws Exception {
        writeCommit(GenericRow.of(1, 1, 1));

        SnapshotManager sm = table.snapshotManager();
        TagManager tagManager = table.tagManager();
        long snapshotId = sm.latestSnapshotId();

        BatchReadTagCreator creator = new BatchReadTagCreator(tagManager, sm, Duration.ofHours(1));

        String tagName = creator.createReadTag(snapshotId);
        assertThat(tagName).isNotNull();
        assertThat(tagName).startsWith(BatchReadTagCreator.BATCH_READ_TAG_PREFIX);
        assertThat(tagManager.tagExists(tagName)).isTrue();

        creator.deleteReadTag(tagName);
        assertThat(tagManager.tagExists(tagName)).isFalse();
    }

    @Test
    public void testIsBatchReadTag() {
        assertThat(BatchReadTagCreator.isBatchReadTag("batch-read-1-abc12345")).isTrue();
        assertThat(BatchReadTagCreator.isBatchReadTag("batch-read-42-xyz")).isTrue();
        assertThat(BatchReadTagCreator.isBatchReadTag("my-tag")).isFalse();
        assertThat(BatchReadTagCreator.isBatchReadTag("2023-07-18 11")).isFalse();
    }

    @Test
    public void testCreateTagFailsGracefully() {
        SnapshotManager sm = table.snapshotManager();
        TagManager tagManager = table.tagManager();

        BatchReadTagCreator creator = new BatchReadTagCreator(tagManager, sm, Duration.ofHours(1));

        // snapshot 999 does not exist
        String tagName = creator.createReadTag(999L);
        assertThat(tagName).isNull();
    }

    @Test
    public void testDeleteNonExistentTagIsNoOp() throws Exception {
        writeCommit(GenericRow.of(1, 1, 1));

        SnapshotManager sm = table.snapshotManager();
        TagManager tagManager = table.tagManager();

        BatchReadTagCreator creator = new BatchReadTagCreator(tagManager, sm, Duration.ofHours(1));

        // should not throw
        creator.deleteReadTag("batch-read-nonexistent-12345678");
    }

    @Test
    public void testScanCreatesProtectionTag() throws Exception {
        writeCommit(GenericRow.of(1, 1, 1));

        Options options = new Options();
        options.set(CoreOptions.SCAN_PLAN_AUTO_TAG_FOR_READ_TIME_RETAINED, Duration.ofHours(2));
        FileStoreTable tableWithOption = table.copy(options.toMap());

        InnerTableScan scan = tableWithOption.newScan();
        TableScan.Plan plan = scan.plan();

        assertThat(plan.splits()).isNotEmpty();
        assertThat(scan).isInstanceOf(DataTableBatchScan.class);

        DataTableBatchScan batchScan = (DataTableBatchScan) scan;
        String tagName = batchScan.readProtectionTagName();
        assertThat(tagName).isNotNull();
        assertThat(tagName).startsWith(BatchReadTagCreator.BATCH_READ_TAG_PREFIX);

        TagManager tagManager = table.tagManager();
        assertThat(tagManager.tagExists(tagName)).isTrue();

        // verify tag has TTL set
        Tag tag = tagManager.getOrThrow(tagName);
        assertThat(tag.getTagTimeRetained()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    public void testScanDoesNotCreateTagWhenDisabled() throws Exception {
        writeCommit(GenericRow.of(1, 1, 1));

        // default: option not set
        InnerTableScan scan = table.newScan();
        scan.plan();

        assertThat(scan).isInstanceOf(DataTableBatchScan.class);
        DataTableBatchScan batchScan = (DataTableBatchScan) scan;
        assertThat(batchScan.readProtectionTagName()).isNull();
    }
}
