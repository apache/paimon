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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.SimpleFileEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for postpone table. */
public class PostponeUtils {

    public static Map<BinaryRow, Integer> getKnownNumBuckets(FileStoreTable table) {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        List<SimpleFileEntry> simpleFileEntries =
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries();
        for (SimpleFileEntry entry : simpleFileEntries) {
            if (entry.totalBuckets() >= 0) {
                Integer oldTotalBuckets =
                        knownNumBuckets.put(entry.partition(), entry.totalBuckets());
                if (oldTotalBuckets != null && oldTotalBuckets != entry.totalBuckets()) {
                    throw new IllegalStateException(
                            "Partition "
                                    + entry.partition()
                                    + " has different totalBuckets "
                                    + oldTotalBuckets
                                    + " and "
                                    + entry.totalBuckets());
                }
            }
        }
        return knownNumBuckets;
    }
}
