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

package org.apache.paimon.hudi;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.clone.HiveCloneExtractor;
import org.apache.paimon.hive.clone.HivePartitionFiles;
import org.apache.paimon.hive.clone.HiveTableCloneExtractor;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link HiveCloneExtractor} for Hudi tables. */
public class HudiHiveCloneExtractor extends HiveTableCloneExtractor {
    private static final Logger LOG = LoggerFactory.getLogger(HudiHiveCloneExtractor.class);
    private static final Set<String> HUDI_METADATA_FIELDS =
            Arrays.stream(HoodieRecord.HoodieMetadataField.values())
                    .map(HoodieRecord.HoodieMetadataField::getFieldName)
                    .collect(Collectors.toSet());

    @Override
    public boolean matches(Table table) {
        if (table.getParameters()
                .getOrDefault("spark.sql.sources.provider", "")
                .equalsIgnoreCase("hudi")) {
            return true;
        }
        // For Hudi version < 0.9, there is no spark-sql support. So we need to check Hudi fields to
        // determine if it is a Hudi table.
        for (FieldSchema field : table.getSd().getCols()) {
            if (HUDI_METADATA_FIELDS.contains(field.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<FieldSchema> extractSchema(
            IMetaStoreClient client, Table hiveTable, String database, String table)
            throws Exception {
        List<FieldSchema> fields = client.getSchema(database, table);
        List<FieldSchema> resultFields =
                fields.stream()
                        .filter(f -> !HUDI_METADATA_FIELDS.contains(f.getName()))
                        .collect(Collectors.toList());
        LOG.info(
                "Hudi table {}.{} with total field count {}, and result field count {} after filter",
                database,
                table,
                fields.size(),
                resultFields.size());
        return resultFields;
    }

    @Override
    public List<HivePartitionFiles> extractFiles(
            Map<String, String> catalogOptions,
            IMetaStoreClient client,
            Table table,
            FileIO fileIO,
            Identifier identifier,
            RowType partitionRowType,
            String defaultPartitionName,
            @Nullable PartitionPredicate predicate) {
        Map<String, String> options = table.getParameters();
        checkTableType(options);

        String location = table.getSd().getLocation();
        HudiFileIndex fileIndex =
                new HudiFileIndex(location, options, catalogOptions, partitionRowType, predicate);

        if (fileIndex.isPartitioned()) {
            return fileIndex.getAllFilteredPartitionFiles(fileIO);
        } else {
            return Collections.singletonList(fileIndex.getUnpartitionedFiles(fileIO));
        }
    }

    @Override
    public boolean supportCloneSplits(String format) {
        return false;
    }

    private static void checkTableType(Map<String, String> conf) {
        String type = conf.getOrDefault("table.type", HoodieTableType.COPY_ON_WRITE.name());
        checkArgument(
                HoodieTableType.valueOf(type) == HoodieTableType.COPY_ON_WRITE,
                "Only Hudi COW table is supported yet but found %s table.",
                type);
    }
}
