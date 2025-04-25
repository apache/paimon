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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/** Extractor to extract partition from the primary key. */
public class PartitionFromPkExtractor {

    private final Projection partitionProjection;

    public PartitionFromPkExtractor(TableSchema schema) {
        this.partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalPrimaryKeysType(),
                        schema.partitionKeys().stream()
                                .mapToInt(schema.primaryKeys()::indexOf)
                                .toArray());
    }

    public BinaryRow partition(InternalRow primaryKey) {
        return partitionProjection.apply(primaryKey);
    }
}
