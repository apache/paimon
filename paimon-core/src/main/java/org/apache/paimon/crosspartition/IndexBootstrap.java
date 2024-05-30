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

package org.apache.paimon.crosspartition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.AbstractDataTableScan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;
import static org.apache.paimon.io.SplitsParallelReadUtil.parallelExecute;

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
        bootstrap(numAssigners, assignId).forEachRemaining(collector);
    }

    public RecordReader<InternalRow> bootstrap(int numAssigners, int assignId) throws IOException {
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        int[] keyProjection =
                table.primaryKeys().stream()
                        .map(fieldNames::indexOf)
                        .mapToInt(Integer::intValue)
                        .toArray();

        // force using the latest scan mode
        ReadBuilder readBuilder =
                table.copy(Collections.singletonMap(SCAN_MODE.key(), LATEST.toString()))
                        .newReadBuilder()
                        .withProjection(keyProjection);

        AbstractDataTableScan tableScan = (AbstractDataTableScan) readBuilder.newScan();
        List<Split> splits =
                tableScan
                        .withBucketFilter(bucket -> bucket % numAssigners == assignId)
                        .plan()
                        .splits();

        CoreOptions options = CoreOptions.fromMap(table.options());
        Duration indexTtl = options.crossPartitionUpsertIndexTtl();
        if (indexTtl != null) {
            long indexTtlMillis = indexTtl.toMillis();
            long currentTime = System.currentTimeMillis();
            splits =
                    splits.stream()
                            .filter(split -> filterSplit(split, indexTtlMillis, currentTime))
                            .collect(Collectors.toList());
        }

        RowDataToObjectArrayConverter partBucketConverter =
                new RowDataToObjectArrayConverter(
                        TypeUtils.concat(
                                TypeUtils.project(rowType, table.partitionKeys()),
                                RowType.of(DataTypes.INT())));

        return parallelExecute(
                TypeUtils.project(rowType, keyProjection),
                s -> readBuilder.newRead().createReader(s),
                splits,
                options.pageSize(),
                options.crossPartitionUpsertBootstrapParallelism(),
                split -> {
                    DataSplit dataSplit = ((DataSplit) split);
                    int bucket = dataSplit.bucket();
                    return partBucketConverter.toGenericRow(
                            new JoinedRow(dataSplit.partition(), GenericRow.of(bucket)));
                },
                (row, extra) -> new JoinedRow().replace(row, extra));
    }

    @VisibleForTesting
    static boolean filterSplit(Split split, long indexTtl, long currentTime) {
        List<DataFileMeta> files = ((DataSplit) split).dataFiles();
        for (DataFileMeta file : files) {
            long fileTime = file.creationTimeEpochMillis();
            if (currentTime <= fileTime + indexTtl) {
                return true;
            }
        }
        return false;
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
