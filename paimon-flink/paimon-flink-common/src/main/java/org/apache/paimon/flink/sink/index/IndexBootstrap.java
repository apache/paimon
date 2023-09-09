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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.AbstractInnerTableScan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;

/** Bootstrap key index from Paimon table. */
public class IndexBootstrap implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String BUCKET_FIELD = "_BUCKET";

    private final Table table;

    public IndexBootstrap(Table table) {
        this.table = table;
    }

    public void bootstrap(int numAssigners, int assignId, Consumer<InternalRow> collector)
            throws IOException {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        List<String> keyPartFields =
                Stream.concat(table.primaryKeys().stream(), table.partitionKeys().stream())
                        .collect(Collectors.toList());
        int[] projection =
                keyPartFields.stream()
                        .map(fieldNames::indexOf)
                        .mapToInt(Integer::intValue)
                        .toArray();

        // force using the latest scan mode
        ReadBuilder readBuilder =
                table.copy(Collections.singletonMap(SCAN_MODE.key(), LATEST.toString()))
                        .newReadBuilder()
                        .withProjection(projection);

        String minPartition =
                CoreOptions.fromMap(table.options()).crossPartitionUpsertBootstrapMinPartition();
        if (minPartition != null) {
            int partIndex = fieldNames.indexOf(table.partitionKeys().get(0));
            Object minPart = TypeUtils.castFromString(minPartition, rowType.getTypeAt(partIndex));
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            readBuilder =
                    readBuilder.withFilter(predicateBuilder.greaterOrEqual(partIndex, minPart));
        }

        AbstractInnerTableScan tableScan = (AbstractInnerTableScan) readBuilder.newScan();
        TableScan.Plan plan =
                tableScan.withBucketFilter(bucket -> bucket % numAssigners == assignId).plan();

        for (Split split : plan.splits()) {
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
                int bucket = ((DataSplit) split).bucket();
                GenericRow bucketRow = GenericRow.of(bucket);
                JoinedRow joinedRow = new JoinedRow();
                reader.transform(row -> joinedRow.replace(row, bucketRow))
                        .forEachRemaining(collector);
            }
        }
    }

    public static RowType bootstrapType(TableSchema schema) {
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<DataField> bootstrapFields =
                new ArrayList<>(
                        schema.projectedLogicalRowType(
                                        Stream.concat(primaryKeys.stream(), partitionKeys.stream())
                                                .collect(Collectors.toList()))
                                .getFields());
        bootstrapFields.add(
                new DataField(
                        RowType.currentHighestFieldId(bootstrapFields) + 1,
                        BUCKET_FIELD,
                        DataTypes.INT().notNull()));
        return new RowType(bootstrapFields);
    }
}
