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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

/** A {@link WriteSelector} for {@link BucketMode#HASH_FIXED}. */
public class FixedBucketWriteSelector implements WriteSelector {

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;

    private transient KeyAndBucketExtractor<InternalRow> extractor;

    public FixedBucketWriteSelector(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public int select(InternalRow row, int numWriters) {
        if (extractor == null) {
            extractor = new FixedBucketRowKeyExtractor(schema);
        }
        extractor.setRecord(row);
        return ChannelComputer.select(extractor.partition(), extractor.bucket(), numWriters);
    }
}
