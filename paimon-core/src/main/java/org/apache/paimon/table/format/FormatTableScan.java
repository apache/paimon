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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    private static final Pattern VALID_FILENAME_PATTERN = Pattern.compile("^[A-Za-z0-9].*");

    private final FormatTable table;
    private Predicate predicate;
    private int[] projection;
    private @Nullable PartitionPredicate partitionFilter;

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
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
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

    public static boolean isDataFileName(String fileName) {
        return fileName != null && !fileName.startsWith(".") && !fileName.startsWith("_");
    }

    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try (FileIO fileIO = table.fileIO()) {
                FileStatus[] files = fileIO.listFiles(new Path(table.location()), true);
                for (FileStatus file : files) {
                    if (isDataFileName(file.getPath().getName())) {
                        FormatDataSplit split =
                                new FormatDataSplit(
                                        file.getPath(),
                                        0,
                                        file.getLen(),
                                        table.rowType(),
                                        file.getModificationTime(),
                                        predicate,
                                        projection);
                        splits.add(split);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }
}
