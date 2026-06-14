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

package org.apache.paimon.spark.write;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * Spark 4.x calls DataWriter.write(metadata, data) for metadata-aware writes. Keep this method in
 * Java so the common sources still compile against Spark 3.5, where that interface method does not
 * exist; Spark 4.x compilation generates the erased bridge required by the runtime call.
 */
public class PaimonV2DataEvolutionDataWriter extends PaimonV2DataEvolutionDataWriterBase {

    public PaimonV2DataEvolutionDataWriter(
            BatchWriteBuilder writeBuilder,
            TableSchema tableSchema,
            StructType writeSchema,
            StructType dataSchema,
            StructType metadataSchema,
            CatalogContext catalogContext) {
        super(writeBuilder, tableSchema, writeSchema, dataSchema, metadataSchema, catalogContext);
    }

    public void write(InternalRow metadata, InternalRow data) {
        writeWithMetadata(metadata, data);
    }
}
