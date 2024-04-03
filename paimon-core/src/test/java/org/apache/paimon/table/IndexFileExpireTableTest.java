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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for expiring index files for dynamic tables. */
public class IndexFileExpireTableTest extends PrimaryKeyTableTestBase {

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, -1);
        return options;
    }

    private Pair<GenericRow, Integer> createRow(int partition, int bucket, int key, int value) {
        return Pair.of(GenericRow.of(partition, key, value), bucket);
    }

    @Test
    public void testIndexFileExpiration() throws Exception {
        prepareExpireTable();

        long indexFileSize = indexFileSize();
        long indexManifestSize = indexManifestSize();

        ExpireSnapshotsImpl expire =
                ((ExpireSnapshots.Expire) table.newExpireSnapshots(table.coreOptions()))
                        .getExpireSnapshots();

        expire.expireUntil(1, 2);
        checkIndexFiles(2);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 1);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 1);

        expire.expireUntil(2, 3);
        checkIndexFiles(3);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 1);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 1);

        expire.expireUntil(3, 5);
        checkIndexFiles(5);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 2);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 2);

        expire.expireUntil(5, 7);
        checkIndexFiles(7);
        assertThat(indexFileSize()).isEqualTo(3);
        assertThat(indexManifestSize()).isEqualTo(1);
    }

    @Test
    public void testIndexFileExpirationWithTag() throws Exception {
        prepareExpireTable();
        ExpireSnapshotsImpl expire =
                ((ExpireSnapshots.Expire) table.newExpireSnapshots(table.coreOptions()))
                        .getExpireSnapshots();

        table.createTag("tag3", 3);
        table.createTag("tag5", 5);

        long indexFileSize = indexFileSize();
        long indexManifestSize = indexManifestSize();

        expire.expireUntil(1, 5);
        checkIndexFiles(5);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 1);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 1);

        expire.expireUntil(5, 7);
        checkIndexFiles(7);
        assertThat(indexFileSize()).isEqualTo(5);
        assertThat(indexManifestSize()).isEqualTo(3);

        TagManager tagManager = new TagManager(LocalFileIO.create(), table.location());
        checkIndexFiles(tagManager.taggedSnapshot("tag3"));
        checkIndexFiles(tagManager.taggedSnapshot("tag5"));
    }

    @Test
    public void testIndexFileExpirationWhenDeletingTag() throws Exception {
        prepareExpireTable();
        ExpireSnapshotsImpl expire =
                ((ExpireSnapshots.Expire) table.newExpireSnapshots(table.coreOptions()))
                        .getExpireSnapshots();

        table.createTag("tag3", 3);
        table.createTag("tag5", 5);

        long indexFileSize = indexFileSize();
        long indexManifestSize = indexManifestSize();

        // if snapshot 3 is not expired, deleting tag won't delete index files
        table.deleteTag("tag3");
        checkIndexFiles(3);
        assertThat(indexFileSize()).isEqualTo(indexFileSize);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize);

        // test delete tag after expiring snapshots
        table.createTag("tag3", 3);
        expire.expireUntil(1, 7);
        table.deleteTag("tag3");

        TagManager tagManager = new TagManager(LocalFileIO.create(), table.location());
        checkIndexFiles(7);
        checkIndexFiles(tagManager.taggedSnapshot("tag5"));
        assertThat(indexFileSize()).isEqualTo(4);
        assertThat(indexManifestSize()).isEqualTo(2);
    }

    @Test
    public void testIndexFileRollbackSnapshot() throws Exception {
        prepareExpireTable();

        long indexFileSize = indexFileSize();
        long indexManifestSize = indexManifestSize();

        table.rollbackTo(5);
        checkIndexFiles(5);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 2);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 2);

        table.rollbackTo(3);
        checkIndexFiles(3);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 3);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 3);

        table.rollbackTo(2);
        checkIndexFiles(2);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 3);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 3);

        table.rollbackTo(1);
        checkIndexFiles(1);
        assertThat(indexFileSize()).isEqualTo(3);
        assertThat(indexManifestSize()).isEqualTo(1);
    }

    @Test
    public void testIndexFileRollbackTag() throws Exception {
        prepareExpireTable();

        long indexFileSize = indexFileSize();
        long indexManifestSize = indexManifestSize();

        table.createTag("tag1", 1);
        table.createTag("tag5", 5);
        table.createTag("tag7", 7);

        table.rollbackTo(5);
        checkIndexFiles(5);
        assertThat(indexFileSize()).isEqualTo(indexFileSize - 2);
        assertThat(indexManifestSize()).isEqualTo(indexManifestSize - 2);

        table.rollbackTo("tag1");
        checkIndexFiles(1);
        assertThat(indexFileSize()).isEqualTo(3);
        assertThat(indexManifestSize()).isEqualTo(1);
    }

    private void prepareExpireTable() throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();

        // commit bucket 1,2,3
        write(write, createRow(1, 1, 1, 1));
        write(write, createRow(2, 2, 2, 2));
        write(write, createRow(3, 3, 3, 3));
        commit.commit(0, write.prepareCommit(true, 0));

        // commit bucket 1 only
        write(write, createRow(1, 1, 2, 2));
        commit.commit(1, write.prepareCommit(true, 1));

        // compact only
        write.compact(row(1), 1, true);
        commit.commit(2, write.prepareCommit(true, 2));

        // commit bucket 2 only
        write(write, createRow(2, 2, 3, 3));
        commit.commit(3, write.prepareCommit(true, 3));

        // commit bucket 2 only
        write(write, createRow(2, 2, 4, 4));
        commit.commit(4, write.prepareCommit(true, 4));

        // commit bucket 2 only
        write(write, createRow(2, 2, 5, 5));
        commit.commit(5, write.prepareCommit(true, 5));

        write.close();
        commit.close();
    }

    private void checkIndexFiles(long snapshotId) {
        checkIndexFiles(table.snapshotManager().snapshot(snapshotId));
    }

    private void checkIndexFiles(Snapshot snapshot) {
        String indexManifest = snapshot.indexManifest();
        IndexFileHandler indexFileHandler = table.store().newIndexFileHandler();
        assertThat(indexFileHandler.existsManifest(indexManifest)).isTrue();

        List<IndexManifestEntry> files = indexFileHandler.readManifest(indexManifest);
        for (IndexManifestEntry file : files) {
            assertThat(indexFileHandler.existsIndexFile(file)).isTrue();
        }
    }

    private long indexFileSize() throws IOException {
        return LocalFileIO.create().listStatus(new Path(table.location(), "index")).length;
    }

    private long indexManifestSize() throws IOException {
        return Arrays.stream(
                        LocalFileIO.create().listStatus(new Path(table.location(), "manifest")))
                .filter(s -> s.getPath().getName().startsWith("index-"))
                .count();
    }

    private void write(StreamTableWrite write, Pair<GenericRow, Integer> rowWithBucket)
            throws Exception {
        write.write(rowWithBucket.getKey(), rowWithBucket.getValue());
    }
}
