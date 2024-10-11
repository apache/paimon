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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link TimeTravelUtil}. */
public class TimeTravelUtilsTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));
        long ts = System.currentTimeMillis();

        write.write(rowData(2, 30, 101L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(3, 50, 500L));
        commit.commit(2, write.prepareCommit(true, 2));

        HashMap<String, String> optMap = new HashMap<>();
        optMap.put("scan.snapshot-id", "2");
        CoreOptions options = CoreOptions.fromMap(optMap);
        Snapshot snapshot = TimeTravelUtil.resolveSnapshotFromOption(options, snapshotManager);
        assertThat(snapshot.id() == 2);

        optMap.clear();
        optMap.put("scan.timestamp-millis", ts + "");
        options = CoreOptions.fromMap(optMap);
        snapshot = TimeTravelUtil.resolveSnapshotFromOption(options, snapshotManager);
        assertThat(snapshot.id() == 1);

        table.createTag("tag3", 3);
        optMap.clear();
        optMap.put("scan.tag-name", "tag3");
        options = CoreOptions.fromMap(optMap);
        snapshot = TimeTravelUtil.resolveSnapshotFromOption(options, snapshotManager);
        assertThat(snapshot.id() == 3);

        // if contain more scan.xxx config would throw out
        optMap.put("scan.snapshot-id", "2");
        CoreOptions options1 = CoreOptions.fromMap(optMap);
        assertThrows(
                IllegalArgumentException.class,
                () -> TimeTravelUtil.resolveSnapshotFromOption(options1, snapshotManager),
                "scan.snapshot-id scan.tag-name scan.watermark and scan.timestamp-millis can contains only one");
        write.close();
        commit.close();
    }
}
