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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Tests for global file index. */
public class GlobalFileIndexTest {

    @Test
    public void testGlobalIndex() throws Exception {
        Table table = null;
        Predicate predicate = null;

        GlobalIndexScanBuilder indexScanBuilder =
                ((FileStoreTable) table).newIndexScanBuilder().withSnapshot(1L);
        Set<Integer> shards = indexScanBuilder.shardList();

        GlobalIndexResult globalFileIndexResult = GlobalIndexResult.ALL;
        for (int i = 0; i < shards.size(); i++) {
            ShardGlobalIndexScanner scanner = indexScanBuilder.withShard(0).build();
            GlobalIndexResult result = scanner.scan(predicate);
            globalFileIndexResult = globalFileIndexResult.and(result);
            if (globalFileIndexResult == GlobalIndexResult.NONE) {
                break;
            }
        }

        List<Long> rowIds;
        if (globalFileIndexResult != GlobalIndexResult.ALL) {
            rowIds = new ArrayList<>();
            globalFileIndexResult
                    .results()
                    .forEachRemaining(range -> range.iterator().forEachRemaining(rowIds::add));
        } else {
            rowIds = null;
        }

        ReadBuilder readBuilder = table.newReadBuilder().withRowIds(rowIds);
        readBuilder.newRead().createReader(readBuilder.newScan().plan());
    }
}
