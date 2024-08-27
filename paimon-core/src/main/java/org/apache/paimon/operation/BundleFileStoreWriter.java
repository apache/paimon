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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;

/** Whether the writer could write bundle. */
public interface BundleFileStoreWriter extends FileStoreWrite<InternalRow> {

    /**
     * Write the batch data to the store according to the partition and bucket.
     *
     * @param partition the partition of the data
     * @param bucket the bucket id of the data
     * @param bundle the given data
     * @throws Exception the thrown exception when writing the record
     */
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;
}
