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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/** The extractor to get partition, index field and row id from records. */
public class RowIdIndexFieldsExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int rowIdPos;
    private final RowType readType;
    private final List<String> partitionKeys;
    private final String indexField;

    private transient Projection lazyPartitionProjection;
    private transient FieldGetter lazyIndexFieldGetter;

    public RowIdIndexFieldsExtractor(
            RowType readType, List<String> partitionKeys, String indexField) {
        this.readType = readType;
        this.partitionKeys = partitionKeys;
        this.indexField = indexField;
        this.rowIdPos = readType.getFieldIndex(SpecialFields.ROW_ID.name());
    }

    private Projection partitionProjection() {
        if (lazyPartitionProjection == null) {
            lazyPartitionProjection = CodeGenUtils.newProjection(readType, partitionKeys);
        }
        return lazyPartitionProjection;
    }

    private FieldGetter indexFieldGetter() {
        if (lazyIndexFieldGetter == null) {
            int indexFieldPos = readType.getFieldIndex(indexField);
            lazyIndexFieldGetter =
                    InternalRow.createFieldGetter(readType.getTypeAt(indexFieldPos), indexFieldPos);
        }
        return lazyIndexFieldGetter;
    }

    public BinaryRow extractPartition(InternalRow record) {
        return partitionProjection().apply(record).copy();
    }

    @Nullable
    public Object extractIndexField(InternalRow record) {
        return indexFieldGetter().getFieldOrNull(record);
    }

    public Long extractRowId(InternalRow record) {
        return record.getLong(rowIdPos);
    }
}
