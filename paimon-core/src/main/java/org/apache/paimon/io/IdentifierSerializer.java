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

package org.apache.paimon.io;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link Identifier}. */
public class IdentifierSerializer extends ObjectSerializer<Identifier> {

    public IdentifierSerializer() {
        super(Identifier.schema());
    }

    @Override
    public InternalRow toRow(Identifier record) {
        return GenericRow.of(
                BinaryString.fromString(record.getDatabaseName()),
                BinaryString.fromString(record.getObjectName()));
    }

    @Override
    public Identifier fromRow(InternalRow rowData) {
        String databaseName = rowData.isNullAt(0) ? null : rowData.getString(0).toString();
        String tableName = rowData.isNullAt(1) ? null : rowData.getString(1).toString();
        return Identifier.create(databaseName, tableName);
    }
}
