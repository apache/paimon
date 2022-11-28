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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.stream.IntStream;

/** A {@link BucketComputer} to compute bucket by bucket keys or primary keys or whole row. */
public class BucketComputer {

    private final int numBucket;

    private final Projection<RowData, BinaryRowData> rowProjection;
    private final Projection<RowData, BinaryRowData> bucketProjection;
    private final Projection<RowData, BinaryRowData> pkProjection;

    public BucketComputer(TableSchema tableSchema) {
        this(
                new CoreOptions(tableSchema.options()).bucket(),
                tableSchema.logicalRowType(),
                tableSchema.projection(tableSchema.originalBucketKeys()),
                tableSchema.projection(tableSchema.trimmedPrimaryKeys()));
    }

    private BucketComputer(int numBucket, RowType rowType, int[] bucketKeys, int[] primaryKeys) {
        this.numBucket = numBucket;
        this.rowProjection =
                CodeGenUtils.newProjection(
                        rowType, IntStream.range(0, rowType.getFieldCount()).toArray());
        this.bucketProjection = CodeGenUtils.newProjection(rowType, bucketKeys);
        this.pkProjection = CodeGenUtils.newProjection(rowType, primaryKeys);
    }

    private int hashRow(RowData row) {
        if (row instanceof BinaryRowData) {
            RowKind rowKind = row.getRowKind();
            row.setRowKind(RowKind.INSERT);
            int hash = hashcode((BinaryRowData) row);
            row.setRowKind(rowKind);
            return hash;
        } else {
            return hashcode(rowProjection.apply(row));
        }
    }

    public int bucket(RowData row) {
        int hashcode = hashBucketKey(row);
        return bucket(hashcode, numBucket);
    }

    public int bucket(RowData row, BinaryRowData pk) {
        int hashcode = hashBucketKey(row, pk);
        return bucket(hashcode, numBucket);
    }

    private int hashBucketKey(RowData row) {
        BinaryRowData bucketKey = bucketProjection.apply(row);
        if (bucketKey.getArity() == 0) {
            bucketKey = pkProjection.apply(row);
        }
        if (bucketKey.getArity() == 0) {
            return hashRow(row);
        }
        return bucketKey.hashCode();
    }

    private int hashBucketKey(RowData row, BinaryRowData pk) {
        BinaryRowData bucketKey = bucketProjection.apply(row);
        if (bucketKey.getArity() == 0) {
            bucketKey = pk;
        }
        if (bucketKey.getArity() == 0) {
            return hashRow(row);
        }
        return bucketKey.hashCode();
    }

    public static int hashcode(BinaryRowData rowData) {
        assert rowData.getRowKind() == RowKind.INSERT;
        return rowData.hashCode();
    }

    public static int bucket(int hashcode, int numBucket) {
        return Math.abs(hashcode % numBucket);
    }
}
