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

package org.apache.paimon.bucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

/**
 * A paimon bucket function that assigns records to buckets using modulo arithmetic: {@code
 * bucket_id = Math.floorMod(bucket_key_value, numBuckets)}. The bucket key must be a single field
 * of INT or BIGINT datatype.
 *
 * <p>Example mappings (numBuckets=5):
 *
 * <pre>
 *   17 → 2 (17 % 5)
 *   -3 → 2 (Math.floorMod(-3, 5))
 *   5 → 0
 * </pre>
 */
public class ModBucketFunction implements BucketFunction {

    private static final long serialVersionUID = 1L;

    private final DataTypeRoot bucketKeyTypeRoot;

    public ModBucketFunction(RowType bucketKeyType) {
        Preconditions.checkArgument(
                bucketKeyType.getFieldCount() == 1,
                "bucket key must have exactly one field in mod bucket function");
        DataTypeRoot bucketKeyTypeRoot = bucketKeyType.getTypeAt(0).getTypeRoot();
        Preconditions.checkArgument(
                bucketKeyTypeRoot == DataTypeRoot.INTEGER
                        || bucketKeyTypeRoot == DataTypeRoot.BIGINT,
                "bucket key type must be INT or BIGINT in mod bucket function, but got %s",
                bucketKeyType.getTypeAt(0));
        this.bucketKeyTypeRoot = bucketKeyTypeRoot;
    }

    @Override
    public int bucket(BinaryRow row, int numBuckets) {
        assert numBuckets > 0
                && row.getRowKind() == RowKind.INSERT
                // for mod bucket function, only one bucket key is supported
                && row.getFieldCount() == 1;
        if (bucketKeyTypeRoot == DataTypeRoot.INTEGER) {
            return Math.floorMod(row.getInt(0), numBuckets);
        } else if (bucketKeyTypeRoot == DataTypeRoot.BIGINT) {
            return (int) Math.floorMod(row.getLong(0), numBuckets);
        } else {
            // shouldn't happen
            throw new UnsupportedOperationException("bucket key type must be INT or BIGINT");
        }
    }
}
