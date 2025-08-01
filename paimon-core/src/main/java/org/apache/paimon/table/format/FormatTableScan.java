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

package org.apache.paimon.table.format;

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    private final FormatTable table;
    private Predicate predicate;
    private int[] projection;

    public FormatTableScan(FormatTable table, Predicate predicate, int[] projection) {
        this.table = table;
        this.predicate = predicate;
        this.projection = projection;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    @Override
    public Plan plan() {
        return new FormatTableScanPlan();
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        throw new UnsupportedOperationException();
    }

    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try {
                // todo: check whether need bucket
                String bucketName = String.format("bucket-%d", table.bucket());
                FileStatus[] files =
                        table.fileIO()
                                .listStatus(
                                        new Path(
                                                String.format(
                                                        "%s/%s/", table.location(), bucketName)));

                for (FileStatus file : files) {
                    FormatDataSplit split =
                            new FormatDataSplit(
                                    table.fileIO(),
                                    file.getPath(),
                                    0,
                                    file.getLen(),
                                    table.rowType(),
                                    predicate,
                                    projection);
                    splits.add(split);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }
}
