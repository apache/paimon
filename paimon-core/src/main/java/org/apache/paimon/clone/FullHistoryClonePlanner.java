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

package org.apache.paimon.clone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.BLOB_DESCRIPTOR_FIELD;
import static org.apache.paimon.CoreOptions.BLOB_VIEW_FIELD;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Creates a fail-fast copy plan for a full-history clone. */
public class FullHistoryClonePlanner {

    private final FileStoreTable sourceTable;
    private final PathMapping pathMapping;

    public FullHistoryClonePlanner(FileStoreTable sourceTable, PathMapping pathMapping) {
        this.sourceTable = sourceTable;
        this.pathMapping = pathMapping;
    }

    public FullHistoryClonePlan plan() throws IOException {
        FullHistoryClonePlan structure = planStructure();
        FullHistoryFileSet fileSet = new FullHistoryFileCollector(sourceTable).collect();
        FullHistoryCopyPlan payloadPlan =
                FullHistoryCopyPlan.buildPayload(fileSet, pathMapping, sourceTable.fileIO());
        return new FullHistoryClonePlan(
                structure.sourceRoot(),
                structure.targetRoot(),
                structure.sourceFingerprint(),
                payloadPlan);
    }

    public FullHistoryClonePlan planStructure() throws IOException {
        validateSupportedSchemas(sourceTable);
        validateSchemaPathMappings(sourceTable, pathMapping);
        Path targetRoot = new Path(pathMapping.rewriteRequired(sourceTable.location().toString()));
        checkArgument(
                !PathMapping.overlaps(sourceTable.location().toString(), targetRoot.toString()),
                "Source and target table roots must not overlap: %s and %s",
                sourceTable.location(),
                targetRoot);
        return new FullHistoryClonePlan(
                sourceTable.location(),
                targetRoot,
                FullHistorySourceFingerprint.compute(sourceTable),
                FullHistoryCopyPlan.empty());
    }

    public static void validateSupportedSchemas(FileStoreTable table) {
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            for (TableSchema schema : table.switchToBranch(branch).schemaManager().listAll()) {
                validateSupportedSchema(schema);
            }
        }
    }

    static void validateSupportedSchema(TableSchema schema) {
        CoreOptions options = CoreOptions.fromMap(schema.options());
        checkArgument(
                options.blobDescriptorField().isEmpty(),
                "Full-history clone does not support %s because its URI is stored inside data files.",
                BLOB_DESCRIPTOR_FIELD.key());
        checkArgument(
                options.blobViewField().isEmpty(),
                "Full-history clone does not support %s because it references another table.",
                BLOB_VIEW_FIELD.key());
        IcebergOptions.StorageType icebergStorage =
                Options.fromMap(schema.options()).get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        checkArgument(
                icebergStorage == IcebergOptions.StorageType.DISABLED,
                "Full-history clone does not support %s=%s because Iceberg compatibility metadata is not copied or rewritten.",
                IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                icebergStorage);
    }

    private static void validateSchemaPathMappings(FileStoreTable table, PathMapping mapping) {
        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            FileStoreTable branchTable = table.switchToBranch(branch);
            for (TableSchema schema : branchTable.schemaManager().listAll()) {
                FullHistoryMetadataRewriter.rewriteOptions(
                        schema.options(), mapping, branchTable.location());
            }
        }
    }
}
