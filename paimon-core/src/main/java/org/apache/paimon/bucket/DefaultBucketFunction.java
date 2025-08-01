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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowKind;

/** Paimon default bucket function. */
public class DefaultBucketFunction implements BucketFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public int bucket(BinaryRow row, int numBuckets) {
        if (numBuckets == BucketMode.UNAWARE_BUCKET) {
            return BucketMode.UNAWARE_BUCKET;
        }
        assert numBuckets > 0 && row.getRowKind() == RowKind.INSERT;
        int hash = row.hashCode();
        return Math.abs(hash % numBuckets);
    }
}
