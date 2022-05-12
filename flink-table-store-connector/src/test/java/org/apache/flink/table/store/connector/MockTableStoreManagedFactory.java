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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.table.store.file.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.DEFAULT_ROW_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.KEY_TYPE;

/** Mock {@link TableStoreManagedFactory} to create {@link TestTableStore} for tests. */
public class MockTableStoreManagedFactory extends TableStoreManagedFactory {

    private final RowType partitionType;
    private final RowType rowType;

    public MockTableStoreManagedFactory() {
        partitionType = DEFAULT_PART_TYPE;
        rowType = DEFAULT_ROW_TYPE;
    }

    public MockTableStoreManagedFactory(RowType partitionType, RowType rowType) {
        this.partitionType = partitionType;
        this.rowType = rowType;
    }

    @Override
    TableStore buildTableStore(Context context) {
        return new TestTableStore(
                context.getObjectIdentifier(),
                Configuration.fromMap(context.getCatalogTable().getOptions()),
                partitionType,
                KEY_TYPE,
                rowType);
    }
}
