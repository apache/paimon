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

    @Override
    public boolean matches(Table table) {
        return table.getParameters()
                .getOrDefault("spark.sql.sources.provider", "")
                .equalsIgnoreCase("hudi");
    }

    @Override
    public List<FieldSchema> extractSchema(
            IMetaStoreClient client, Table hiveTable, String database, String table)
            throws Exception {
        List<FieldSchema> fields = client.getSchema(database, table);
        Set<String> hudiMetadataFields =
                Arrays.stream(HoodieRecord.HoodieMetadataField.values())
                        .map(HoodieRecord.HoodieMetadataField::getFieldName)
                        .collect(Collectors.toSet());
        return fields.stream()
                .filter(f -> !hudiMetadataFields.contains(f.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public List<HivePartitionFiles> extractFiles(
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
        HudiFileIndex fileIndex = new HudiFileIndex(location, options, partitionRowType, predicate);

        if (fileIndex.isPartitioned()) {
            return fileIndex.getAllFilteredPartitionFiles(fileIO);
        } else {
            return Collections.singletonList(fileIndex.getUnpartitionedFiles(fileIO));
        }
    }

    private static void checkTableType(Map<String, String> conf) {
        String type = conf.getOrDefault("table.type", HoodieTableType.COPY_ON_WRITE.name());
        checkArgument(
                HoodieTableType.valueOf(type) == HoodieTableType.COPY_ON_WRITE,
                "Only Hudi COW table is supported yet but found %s table.",
                type);
    }
}
