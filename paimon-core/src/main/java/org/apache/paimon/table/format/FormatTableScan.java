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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryWriter;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

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
        List<Pair<LinkedHashMap<String, String>, Path>> partition2Paths =
                PartitionPathUtils.searchPartSpecAndPaths(
                        table.fileIO(), new Path(table.location()), table.partitionKeys().size());
        List<PartitionEntry> partitionEntries = new ArrayList<>();
        for (Pair<LinkedHashMap<String, String>, Path> partition2Path : partition2Paths) {
            BinaryRow row = createPartitionRow(partition2Path.getKey());
            partitionEntries.add(new PartitionEntry(row, -1L, -1L, -1L, -1L));
        }
        return partitionEntries;
    }

    public static boolean isDataFileName(String fileName) {
        return fileName != null && !fileName.startsWith(".") && !fileName.startsWith("_");
    }

    private BinaryRow createPartitionRow(LinkedHashMap<String, String> partitionSpec) {
        RowType partitionRowType = table.partitionType();
        List<DataField> fields = partitionRowType.getFields();

        // Create value setters for each field type
        List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();
        for (DataField field : fields) {
            valueSetters.add(BinaryWriter.createValueSetter(field.type()));
        }

        // Create binary row to hold partition values
        BinaryRow binaryRow = new BinaryRow(fields.size());
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        // Fill in partition values
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i).name();
            String partitionValue = partitionSpec.get(fieldName);

            if (partitionValue == null || partitionValue.equals(table.defaultPartName())) {
                // Set null for default partition name or missing partition
                binaryRowWriter.setNullAt(i);
            } else {
                // Convert string value to appropriate type and set it
                Object value = TypeUtils.castFromString(partitionValue, fields.get(i).type());
                valueSetters.get(i).setValue(binaryRowWriter, i, value);
            }
        }

        binaryRowWriter.complete();
        return binaryRow;
    }

    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try (FileIO fileIO = table.fileIO()) {
                if (partitionFilter != null && !table.partitionKeys().isEmpty()) {
                    List<Pair<LinkedHashMap<String, String>, Path>> partition2Paths =
                            PartitionPathUtils.searchPartSpecAndPaths(
                                    fileIO,
                                    new Path(table.location()),
                                    table.partitionKeys().size());
                    for (Pair<LinkedHashMap<String, String>, Path> partition2Path :
                            partition2Paths) {
                        LinkedHashMap<String, String> partitionSpec = partition2Path.getKey();
                        BinaryRow partitionRow = createPartitionRow(partitionSpec);
                        if (partitionFilter.test(partitionRow)) {
                            FileStatus[] files = fileIO.listStatus(partition2Path.getValue());
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
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }
}
