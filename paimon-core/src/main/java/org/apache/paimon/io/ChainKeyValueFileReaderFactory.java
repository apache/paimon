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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;

import java.util.HashMap;
import java.util.Map;

/** A specific implementation about {@link KeyValueFileReaderFactory} for chain read. */
public class ChainKeyValueFileReaderFactory extends KeyValueFileReaderFactory {

    private final ChainReadContext chainReadContext;

    private final String currentBranch;

    private final Map<String, SchemaManager> branchSchemaManagers;

    public ChainKeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FormatReaderMapping.Builder formatReaderMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory,
            ChainReadContext chainReadContext) {
        super(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatReaderMappingBuilder,
                pathFactory,
                asyncThreshold,
                partition,
                dvFactory);
        this.chainReadContext = chainReadContext;
        CoreOptions options = new CoreOptions(schema.options());
        this.currentBranch = options.branch();
        this.branchSchemaManagers = new HashMap<>();
        this.branchSchemaManagers.put(currentBranch, schemaManager);
    }

    @Override
    protected TableSchema getDataSchema(DataFileMeta fileMeta) {
        String branch = chainReadContext.fileBranchMapping().get(fileMeta.fileName());
        if (currentBranch.equalsIgnoreCase(branch)) {
            super.getDataSchema(fileMeta);
        }
        if (!branchSchemaManagers.containsKey(branch)) {
            throw new RuntimeException("No schema manager found for branch: " + branch);
        }
        return branchSchemaManagers.get(branch).schema(fileMeta.schemaId());
    }

    @Override
    protected BinaryRow getLogicalPartition() {
        return chainReadContext.logicalPartition();
    }
}
