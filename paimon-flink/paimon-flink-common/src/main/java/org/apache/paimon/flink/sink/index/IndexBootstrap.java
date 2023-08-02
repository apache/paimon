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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.AbstractInnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Bootstrap key index from Paimon table. */
public class IndexBootstrap implements Serializable {

    private static final long serialVersionUID = 1L;

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
        FieldGetter[] keyPartGetters = new FieldGetter[keyPartFields.size()];
        for (int i = 0; i < keyPartGetters.length; i++) {
            keyPartGetters[i] =
                    InternalRow.createFieldGetter(
                            rowType.getTypeAt(fieldNames.indexOf(keyPartFields.get(i))), i);
        }

        int[] projection =
                keyPartFields.stream()
                        .map(fieldNames::indexOf)
                        .mapToInt(Integer::intValue)
                        .toArray();
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(projection);

        AbstractInnerTableScan tableScan = (AbstractInnerTableScan) readBuilder.newScan();
        TableScan.Plan plan =
                tableScan.withBucketFilter(bucket -> bucket % numAssigners == assignId).plan();

        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(
                    keyRow -> {
                        GenericRow row = new GenericRow(fieldNames.size());
                        for (int i = 0; i < keyPartGetters.length; i++) {
                            row.setField(projection[i], keyPartGetters[i].getFieldOrNull(keyRow));
                        }
                        collector.accept(row);
                    });
        }
    }
}
