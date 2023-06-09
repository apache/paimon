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

package org.apache.paimon.flink.sink;

import org.apache.paimon.schema.TableSchema;

import org.apache.flink.table.data.RowData;

/** Hash key of a {@link RowData}. */
public class RowHashKeyChannelComputer implements ChannelComputer<RowData> {

    private static final long serialVersionUID = 1L;

    private final TableSchema schema;

    private transient int numChannels;
    private transient RowDataPartitionKeyExtractor extractor;

    public RowHashKeyChannelComputer(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public void setup(int numChannels) {
        this.numChannels = numChannels;
        this.extractor = new RowDataPartitionKeyExtractor(schema);
    }

    @Override
    public int channel(RowData record) {
        int hash = extractor.trimmedPrimaryKey(record).hashCode();
        return Math.abs(hash % numChannels);
    }

    @Override
    public String toString() {
        return "shuffle by key hash";
    }
}
