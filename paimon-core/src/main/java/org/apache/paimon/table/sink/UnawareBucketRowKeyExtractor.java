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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Unaware bucket table put all records into bucket 0 (for comparability with old design). */
public class UnawareBucketRowKeyExtractor extends RowKeyExtractor {

    public UnawareBucketRowKeyExtractor(TableSchema schema) {
        super(schema);
        int numBuckets = new CoreOptions(schema.options()).bucket();
        checkArgument(
                numBuckets == -1,
                "Only 'bucket' = '-1' is allowed for 'UnawareBucketRowKeyExtractor', but found: "
                        + numBuckets);
    }

    @Override
    public int bucket() {
        return BucketMode.UNAWARE_BUCKET;
    }
}
